package ru.sbt.sup.jdbc.adapter;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.sbt.sup.jdbc.Client;
import ru.sbt.sup.jdbc.config.FormatSpec;
import ru.sbt.sup.jdbc.config.TypeSpec;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

public class LakeS3Adapter {

    private static final Logger logger = LogManager.getLogger(Client.class);

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
        //this.s3Source = new AmazonS3URI(source);
        //this.s3Client = s3Client();
        this.s3Source = s3Source;
        this.s3Client = s3Client;
        this.query = compileQuery(projects, filters);
        logger.info("S3 Query= " + query + "\n");
    }

    public String compileQuery(int[] projects, List<RexNode> filters) {
        return compileSelectFromClause(projects) + compileWhereClause(filters);
    }

    private static String compileWhereClause(List<RexNode> filters) {
        StringBuffer result = new StringBuffer();
        List<String> handledFilters = new ArrayList<>();
        List<RexNode> unhandledFilters = new ArrayList<>();
        if (filters.size()==1){
            RexNode filter = filters.get(0);
            if (RelOptUtil.disjunctions(filter).size() == 1) {
                performConjunction(filter, handledFilters, unhandledFilters);
                result.append(String.join(" AND ", handledFilters));
                filters.clear();
                filters.addAll(unhandledFilters);
            } else {
                List<RexNode> disjunctions = RelOptUtil.disjunctions(filter);
                int disj = 0;
                for (RexNode disjunction : disjunctions) {
                    performConjunction(disjunction, handledFilters, unhandledFilters);
                    if (!unhandledFilters.isEmpty()) {
                        result.setLength(0);
                        break;
                    }
                    if (!handledFilters.isEmpty()){
                        if (++disj > 1) result.append(" OR ");
                        result.append(String.join(" AND ", handledFilters));
                        handledFilters.clear();
                    }
                }
                if (result.length()>0) {
                    filters.clear();
                }
            }
        }
        return (result.length()>0) ? " WHERE " + result : "";
    }

    private static void performConjunction(RexNode node, List<String> handledFilters, List<RexNode> unhandledFilters){
        List<RexNode> conjunctions = RelOptUtil.conjunctions(node);
        for (RexNode conjunction : conjunctions) {
            tryFilterConversion(conjunction).ifPresentOrElse(
                handledFilters::add,
                () -> unhandledFilters.add(conjunction));
        }
    }

    private static Optional<String> tryFilterConversion(RexNode node) {
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
                return tryOperatorFilterConversion("in", call);
//            case NOT:
//                return tryOperatorFilterConversion("not", call);
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> tryOperatorFilterConversion(String op, RexCall call) {
        List<RexNode> operands = call.getOperands();
        RexNode originalLeft = operands.get(0);
        RexNode originalRight = operands.get(1);
        RexNode left = unwrapCasts(originalLeft);
        RexNode right = unwrapCasts(originalRight);
        final String fieldName;
        final String literal;
        if (op.equals("in")){
            fieldName = String.format("%s", compileFieldName(left));
            RexLiteral rlit = (RexLiteral) right;
            if (CalciteUtils.isSarg(rlit)){
                List inList = Lists.transform(CalciteUtils.sargValue(rlit), Object::toString);
                literal = String.join(",", inList);
            } else {
                return Optional.empty();
            }
            return Optional.of(String.format("%s %s (%s)", fieldName, op, literal));
        } else {
            if (isSimpleLiteralColumnValueFilter(left, right)) {
                fieldName = String.format("%s", compileFieldName(left));
                literal = String.format("%s", compileLiteral(right));
            } else {
                if (isSimpleLiteralColumnValueFilter(right, left)) {
                    fieldName = String.format("%s", compileFieldName(right));
                    literal = String.format("%s", compileLiteral(left));
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

    private static String compileFieldName(RexNode node) {
        int index = ((RexInputRef) node).getIndex() + 1;
        return "_" + index;
    }

    private static String compileLiteral(RexNode node) {
        RexLiteral literal = (RexLiteral) node;
        if (SqlTypeName.STRING_TYPES.contains(literal.getTypeName())) {
            return '\'' + literal.getValue2().toString() + '\'';
        }
        return literal.getValue2().toString();
    }

    static String compileSelectFromClause(int[] projects) {
        String selectList = IntStream.of(projects).boxed()
                .map(i -> i + 1).map(i -> "_" + i)
                .collect(Collectors.joining(", "));
        return String.format("SELECT %s FROM S3Object", selectList);
    }

    public RowConverter getRowConverter() {
        TypeSpec[] projectedTypes = IntStream.of(projects).boxed()
                .map(i -> types[i])
                .toArray(TypeSpec[]::new);
        return new RowConverter(projectedTypes);
    }

    public InputStream getResult() {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(s3Source.getBucket());
        request.setKey(s3Source.getKey());
        request.setExpression(query);
        //request.setExpression("select * from S3Object[*][*] s"); // <-- working example on people.json aws
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
        csvInput.setFieldDelimiter(spec.delimiter);
        csvInput.setRecordDelimiter(spec.lineSeparator);
        csvInput.setQuoteCharacter(spec.quoteChar);
        csvInput.setQuoteEscapeCharacter(spec.escape);
        csvInput.setFileHeaderInfo(spec.header ? FileHeaderInfo.USE : FileHeaderInfo.NONE);
        csvInput.setComments(spec.commentChar);
        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        switch (spec.compression) {
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
        switch (spec.compression) {
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
        csvOutput.setFieldDelimiter(spec.delimiter);
        csvOutput.setRecordDelimiter(spec.lineSeparator);
        csvOutput.setQuoteCharacter(spec.quoteChar);
        csvOutput.setQuoteEscapeCharacter(spec.escape);
        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);
        return outputSerialization;
    }

    public FormatSpec getFormat() { return format; }
}
