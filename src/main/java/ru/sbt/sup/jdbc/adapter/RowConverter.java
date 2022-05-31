package ru.sbt.sup.jdbc.adapter;

import ru.sbt.sup.jdbc.config.FormatCSVSpec;
import ru.sbt.sup.jdbc.config.TypeSpec;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

public class RowConverter {

    private final FormatCSVSpec formatCSVSpec;
    private final TypeSpec[] projectedFieldTypes;

    public RowConverter(FormatCSVSpec formatCSVSpec, TypeSpec[] projectedFieldTypes) {
        this.formatCSVSpec = formatCSVSpec;
        this.projectedFieldTypes = projectedFieldTypes;
    }

    Object convertField(TypeSpec type, String value) throws NumberFormatException, DateTimeParseException {
        switch (type) {
            case STRING:
                return value;
            case CHARACTER:
                if (value.length() == 1) {
                    return value.charAt(0);
                } else {
                    throw new IllegalArgumentException("invalid char string value: '" + value + "'");
                }
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case BYTE:
                return Byte.parseByte(value);
            case SHORT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case DATE: {
                LocalDate ld = LocalDate.parse(value, DateTimeFormatter.ofPattern(formatCSVSpec.getDatePattern()));
                return Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            case TIME:
                return DateTimeFormatter.ISO_LOCAL_TIME.parse(value, LocalTime::from);
            case DATETIME:
                return DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(value, LocalDateTime::from);
            case TIMESTAMP:
                return DateTimeFormatter.ISO_INSTANT.parse(value, Instant::from);
            default:
                throw new IllegalArgumentException("invalid field type: " + type);
        }
    }

    public Object[] convertRow(String[] values) {
        final Object[] result = new Object[projectedFieldTypes.length];
        for (int i = 0; i < projectedFieldTypes.length; i++) {
            String value = values[i];
            if (value != null) {
                TypeSpec type = projectedFieldTypes[i];
                result[i] = convertField(type, value);
            }
        }
        return result;
    }
}
