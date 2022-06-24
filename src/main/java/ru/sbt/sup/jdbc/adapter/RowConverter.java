package ru.sbt.sup.jdbc.adapter;

import ru.sbt.sup.jdbc.config.FormatCSVSpec;
import ru.sbt.sup.jdbc.config.TypeSpec;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    Object convertField(TypeSpec type, String value) {
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
                try {
                    return new java.sql.Date(new SimpleDateFormat(formatCSVSpec.getDatePattern()).parse(value).getTime());
                } catch (ParseException e){
                    throw new RuntimeException(e);
                }
            }
            case TIME:
                try {
                    return new SimpleDateFormat(formatCSVSpec.getTimePattern()).parse(value);
                } catch (ParseException e){
                    throw new RuntimeException(e);
                }
            case DATETIME:
                try {
                    return new SimpleDateFormat(formatCSVSpec.getDatetimePattern()).parse(value);
                } catch (ParseException e){
                    throw new RuntimeException(e);
                }
            case TIMESTAMP:
                try {
                    return new SimpleDateFormat(formatCSVSpec.getTimestampPattern()).parse(value);
                } catch (ParseException e){
                    throw new RuntimeException(e);
                }
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
