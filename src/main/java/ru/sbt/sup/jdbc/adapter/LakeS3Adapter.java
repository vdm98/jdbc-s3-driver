package ru.sbt.sup.jdbc.adapter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.sbt.sup.jdbc.config.FormatSpec;
import ru.sbt.sup.jdbc.config.TypeSpec;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

public class LakeS3Adapter {

    private static final Logger logger = LogManager.getLogger(LakeS3Adapter.class);

    private String query;
    private FormatSpec format;
    private TypeSpec[] types;
    private int[] projects;
    private AmazonS3URI s3Source;
    private AmazonS3 s3Client;

    public LakeS3Adapter(AmazonS3 s3Client, AmazonS3URI s3Source, FormatSpec format, TypeSpec[] types, int[] projects, List<RexNode> filters) {
        if (filters.size()>0){
            logger.info("\nFilter size= " + filters.size()+"\nFilter [0]= " + filters.get(0).toString());
        }
        this.format = format;
        this.types = types;
        this.projects = projects;
        this.s3Source = s3Source;
        this.s3Client = s3Client;
        this.query = compileQuery(projects, filters);
        logger.info("S3 Query= " + query + "\n");
    }

    public String compileQuery(int[] projects, List<RexNode> filters) {
        return compileSelectFromClause(projects) + compileWhereClause(filters);
    }

    private String compileWhereClause(List<RexNode> filters) {
        StringBuffer result = new StringBuffer();
        List<RexNode> unhandledFilters = new ArrayList<>();
        boolean pushdown = true;
        List<RexNode> originalFilters = new ArrayList<RexNode>();
        originalFilters.addAll(filters);

        if (filters.size()==1){
            RexNode filter = filters.get(0);
            if (RelOptUtil.disjunctions(filter).size() == 1) {
                performConjunction(filter, result, unhandledFilters);
                // store unhandled filters
                filters.clear();
                filters.addAll(unhandledFilters);
            } else {
                performDisjunction(filter, result, unhandledFilters);
                if (!unhandledFilters.isEmpty()) {
                    result.setLength(0);
                    // restore original filters
                    filters.clear();
                    filters.addAll(originalFilters);
                }
            }
        }
        return (result.length()>0) ? " WHERE " + result : "";
    }

    private void performConjunction(RexNode rexNode, StringBuffer result, List<RexNode> unhandledFilters){
        List<RexNode> conjunctions = RelOptUtil.conjunctions(rexNode);
        int conj = 0;
        for (RexNode node : conjunctions) {
            ++conj;
            List<RexNode> disjunctions = RelOptUtil.disjunctions(node);
            if (disjunctions.size() > 1) {
                performDisjunction(node, result, unhandledFilters);
            }
            if (node.getKind().name().equals("NOT")){
                result.append(" NOT ");
                node = ((RexCall)node).getOperands().get(0);
            }
            Optional<String> filter = tryFilterConversion(node);
            if (filter.isPresent()){
                if (conj > 1) result.append(" AND ");
                result.append(filter.get());
            } else {
                unhandledFilters.add(node);
            }
        }
    }

    private void performDisjunction(RexNode node, StringBuffer result, List<RexNode> unhandledFilters) {
        List<RexNode> disjunctions = RelOptUtil.disjunctions(node);
        int disj = 0;
        result.append("(");
        for (RexNode disjunction : disjunctions) {
            if (++disj > 1) result.append(" OR ");
            performConjunction(disjunction, result, unhandledFilters);
        }
        result.append(")");
    }

    private Optional<String> tryFilterConversion(RexNode node) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
            case NOT_EQUALS:
                return tryOperatorFilterConversion("<>", call);
            case EQUALS:
                return tryOperatorFilterConversion("=", call);
            case LESS_THAN:
                return tryOperatorFilterConversion("<", call);
            case LESS_THAN_OR_EQUAL:
                return tryOperatorFilterConversion("<=", call);
            case GREATER_THAN:
                return tryOperatorFilterConversion(">", call);
            case GREATER_THAN_OR_EQUAL:
                return tryOperatorFilterConversion(">=", call);
            case LIKE:
                return tryOperatorFilterConversion("like", call);
            case SEARCH:
                return tryOperatorFilterConversion("search", call);
            default:
                return Optional.empty();
        }
    }

    private Optional<String> tryOperatorFilterConversion(String op, RexCall call) {
        List<RexNode> operands = call.getOperands();
        RexNode originalLeft = operands.get(0);
        RexNode originalRight = operands.get(1);
        RexNode left = unwrapCasts(originalLeft);
        RexNode right = unwrapCasts(originalRight);
        final String fieldName;
        final String literal;
        if (op.equals("search")){
            fieldName = compileFieldName(left, right);
            RexLiteral rlit = (RexLiteral) right;
            if (CalciteUtils.isSarg(rlit)){
                // in operator
                List<Object> sargList = CalciteUtils.sargValue(rlit);
                if (!sargList.isEmpty()) {
                    List inList = Lists.transform(sargList, Object::toString);
                    literal = String.join(",", inList);
                    return Optional.of(String.format("%s %s (%s)", fieldName, "in", literal));
                } else {
                    // ranges
                    final Sarg sarg = rlit.getValueAs(Sarg.class);
                    Set<Range> ranges = sarg.rangeSet.asRanges();
                    StringBuffer rangeStr = new StringBuffer();
                    ranges.forEach(range -> {
                        if (rangeStr.length() > 0) rangeStr.append(" AND ");
                        appendSargFieldName(range.lowerEndpoint(), fieldName, rangeStr);
                        rangeStr.append(" > ");
                        appendSargFieldValue(range.lowerEndpoint(), rangeStr);
                        rangeStr.append(" AND ");
                        appendSargFieldName(range.upperEndpoint(), fieldName, rangeStr);
                        rangeStr.append(" < ");
                        appendSargFieldValue(range.upperEndpoint(), rangeStr);
                    });
                    return Optional.of(rangeStr.toString());
                }
            } else {
                return Optional.empty();
            }
        } else {
            if (isSimpleLiteralColumnValueFilter(left, right)) {
                fieldName = compileFieldName(left, right);
                literal = compileFieldValue(right);
            } else {
                if (isSimpleLiteralColumnValueFilter(right, left)) {
                    fieldName =  compileFieldName(right, left);
                    literal = compileFieldValue(left);
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(String.format("%s %s %s", fieldName, op, literal));
        }
    }

    private static boolean isSimpleLiteralColumnValueFilter(RexNode left, RexNode right) {
        return left.getKind() == INPUT_REF && right.getKind() == LITERAL;
    }

    private static RexNode unwrapCasts(RexNode node) {
        while (node.isA(SqlKind.CAST)) {
            RexCall call = (RexCall) node;
            List<RexNode> operands = call.getOperands();
            node = operands.get(0);
        }
        return node;
    }

    private String compileFieldName(RexNode field, RexNode value) {
        int index = ((RexInputRef) field).getIndex() + 1;
        RexLiteral literal = (RexLiteral) value;
        if (SqlTypeName.DATETIME_TYPES.contains(literal.getTypeName())){
            return "TO_TIMESTAMP("+"_" + index + ", '" + format.getDatePattern() + "')";
        } else if (literal.getType().toString().startsWith("DECIMAL")){
            return "CAST (_" + index + " AS DECIMAL)";
        }  else if (literal.getType().toString().equals("INTEGER")){
            return "CAST (_" + index + " AS INTEGER)";
        } else {
            return "_" + index;
        }
    }

    private String compileFieldValue(RexNode value) {
        RexLiteral literal = (RexLiteral) value;
        if (SqlTypeName.STRING_TYPES.contains(literal.getTypeName())) {
            return '\'' + literal.getValue2().toString() + '\'';
        } else if (SqlTypeName.DATETIME_TYPES.contains(literal.getTypeName())){
            return "TO_TIMESTAMP('" + literal.toString().replaceAll("/","-")+"', 'y-M-d H:m:ss')";
        }
        return literal.getValue().toString(); // was getValue2 !!!!!!!!!!
    }

    private String compileSelectFromClause(int[] projects) {
        String selectList = IntStream.of(projects).boxed()
                .map(i -> i + 1).map(i -> "_" + i)
                .collect(Collectors.joining(", "));
        return String.format("SELECT %s FROM S3Object", selectList);
    }

    private void appendSargFieldName(Comparable endpoint, String fieldName, StringBuffer rangeStr) {
        if (endpoint instanceof TimestampString) {
            rangeStr.append("TO_TIMESTAMP(" + fieldName + ", '" + format.getDatePattern() + "')");
        } else {
            rangeStr.append(fieldName);
        }
    }

    private void appendSargFieldValue(Comparable endpoint, StringBuffer rangeStr) {
        if (endpoint instanceof TimestampString) {
            rangeStr.append("TO_TIMESTAMP('" + endpoint + "', 'y-M-d H:m:ss')");
        } else {
            rangeStr.append(endpoint);
        }
    }


    public RowConverter getRowConverter() {
        TypeSpec[] projectedTypes = IntStream.of(projects).boxed()
                .map(i -> types[i])
                .toArray(TypeSpec[]::new);
        return new RowConverter(format, projectedTypes);
    }

    public InputStream getS3Result() {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(s3Source.getBucket());
        request.setKey(s3Source.getKey());
        request.setExpression(query);
        //request.setExpression("select * from S3Object[*][*] s"); // <-- working example on people.json aws
        //request.setExpression("SELECT _1, _2, _3 FROM S3Object WHERE CAST(_1 AS INTEGER) = 1");
        //request.setExpression("SELECT _1, _2, _3 FROM S3Object WHERE (_2 like 'b%' OR _2 in ('ccc','ddd')) AND _1 > 2");
        //request.setExpression("SELECT _7, _2, _8 FROM S3Object WHERE CAST('2020-01-01T' AS TIMESTAMP) < CAST('2021-01-01T' AS TIMESTAMP)");
        //request.setExpression("SELECT _7, _2, _8 FROM S3Object WHERE TO_TIMESTAMP(_8, 'M/d/y') > TO_TIMESTAMP('2014-01-20 00:00:00', 'y-M-d H:m:ss')");
        request.setExpressionType(ExpressionType.SQL);
        request.setInputSerialization(getInputSerialization(format));
        //request.setInputSerialization(getJsonInputSerialization(format));
        request.setOutputSerialization(getOutputSerialization(format));
        SelectObjectContentResult result = s3Client.selectObjectContent(request);
        SelectObjectContentEventStream payload = result.getPayload();
//        try {
//            logger.info("inputstream result= " + new String(payload.getRecordsInputStream().readAllBytes()));
//            System.exit(0);
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        return payload.getRecordsInputStream();
    }

    private static InputSerialization getInputSerialization(FormatSpec spec) {
        CSVInput csvInput = new CSVInput();
        csvInput.setFieldDelimiter(spec.getDelimiter());
        csvInput.setRecordDelimiter(spec.getLineSeparator());
        csvInput.setQuoteCharacter(spec.getQuoteChar());
        csvInput.setQuoteEscapeCharacter(spec.getEscape());
        csvInput.setFileHeaderInfo(spec.isHeader() ? FileHeaderInfo.USE : FileHeaderInfo.NONE);
        csvInput.setComments(spec.getCommentChar());
        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        switch (spec.getCompression()) {
            case GZIP:
                inputSerialization.setCompressionType(CompressionType.GZIP);
                break;
            case NONE:
            default:
                inputSerialization.setCompressionType(CompressionType.NONE);
        }
        return inputSerialization;
    }

    private static InputSerialization getJsonInputSerialization(FormatSpec spec) {
        JSONInput jsonInput = new JSONInput();
        jsonInput.setType(JSONType.DOCUMENT);
        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setJson(jsonInput);
        switch (spec.getCompression()) {
            case GZIP:
                inputSerialization.setCompressionType(CompressionType.GZIP);
                break;
            case NONE:
            default:
                inputSerialization.setCompressionType(CompressionType.NONE);
        }
        return inputSerialization;
    }

    private static OutputSerialization getOutputSerialization(FormatSpec spec) {
        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(spec.getDelimiter());
        csvOutput.setRecordDelimiter(spec.getLineSeparator());
        csvOutput.setQuoteCharacter(spec.getQuoteChar());
        csvOutput.setQuoteEscapeCharacter(spec.getEscape());
        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);
        return outputSerialization;
    }

    public FormatSpec getFormat() { return format; }
}
