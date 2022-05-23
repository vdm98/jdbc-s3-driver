package ru.sbt.sup.jdbc.adapter;

import com.google.common.collect.Range;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CalciteUtils {

    public static boolean isSarg(RexLiteral literal) {
        return SqlTypeName.SARG.getName().equalsIgnoreCase(literal.getTypeName().getName());
    }


    public static List<Object> sargValue(RexLiteral literal) {
        final Sarg sarg = literal.getValueAs(Sarg.class);
        final RelDataType type = literal.getType();
        List<Object> values = new ArrayList<>();
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        if (sarg.isPoints()) {
            // ref = value or ref IN (value1, ...)
            Set<Range> ranges = sarg.rangeSet.asRanges();
            ranges.forEach(range ->
                    values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
        } else if (sarg.isComplementedPoints()) {
            // ref <> value or ref NOT IN (value1, ...)
            Set<Range> ranges = sarg.negate().rangeSet.asRanges();
            ranges.forEach(range ->
                    values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
        }
        return values;
    }

    private static Object sargPointValue(Object point, SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case CHAR:
            case VARCHAR:
                return "'"+((NlsString) point).getValue()+"'";
            default:
                return point;
        }
    }
}
