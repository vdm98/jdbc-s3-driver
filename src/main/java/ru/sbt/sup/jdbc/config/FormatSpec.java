package ru.sbt.sup.jdbc.config;

import javax.json.Json;
import javax.json.JsonObject;

public class FormatSpec {

    private final char delimiter;
    private final String lineSeparator;
    private final char quoteChar;
    private final char escape;
    private final char commentChar;
    private final boolean header;
    private final boolean strictQuotes;
    private final boolean ignoreLeadingWhiteSpace;
    private final boolean ignoreQuotations;
    private final NullFieldIndicator nullFieldIndicator;
    private final CompressionType compression;
    private final String datePattern;
    private final String timePattern;
    private final String datetimePattern;
    private final String timestampPattern;

    FormatSpec(JsonObject object) {
        this.delimiter = object.getString("delimiter").charAt(0);
        this.lineSeparator = object.getString("lineSeparator");
        this.quoteChar = object.getString("quoteChar").charAt(0);
        this.escape = object.getString("escape").charAt(0);
        this.commentChar = object.getString("commentChar").charAt(0);
        this.header = object.getBoolean("header");
        this.strictQuotes = object.getBoolean("strictQuotes");
        this.ignoreLeadingWhiteSpace = object.getBoolean("ignoreLeadingWhiteSpace");
        this.ignoreQuotations = object.getBoolean("ignoreQuotations");
        this.nullFieldIndicator = NullFieldIndicator.valueOf(object.getString("nullFieldIndicator").toUpperCase());
        this.compression = CompressionType.valueOf(object.getString("compression").toUpperCase());
        this.datePattern = object.getString("datePattern");
        this.timePattern = object.getString("timePattern");
        this.datetimePattern = object.getString("datetimePattern");
        this.timestampPattern = object.getString("timestampPattern");
    }

    public enum CompressionType {
        NONE,
        GZIP
    }

    public enum NullFieldIndicator {
        EMPTY_SEPARATORS,
        EMPTY_QUOTES,
        BOTH,
        NEITHER
    }

    JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("delimiter", "" + delimiter)
                .add("lineSeparator", lineSeparator)
                .add("quoteChar", "" + quoteChar)
                .add("escape", "" + escape)
                .add("commentChar", "" + commentChar)
                .add("header", header)
                .add("strictQuotes", strictQuotes)
                .add("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
                .add("ignoreQuotations", ignoreQuotations)
                .add("nullFieldIndicator", nullFieldIndicator.name().toLowerCase())
                .add("compression", compression.name().toLowerCase())
                .add("datePattern", datePattern)
                .add("timePattern", timePattern)
                .add("datetimePattern", datetimePattern)
                .add("timestampPattern", timestampPattern)
                .build();
    }

    public char getDelimiter() {
        return delimiter;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public char getEscape() {
        return escape;
    }

    public char getCommentChar() {
        return commentChar;
    }

    public boolean isHeader() {
        return header;
    }

    public boolean isStrictQuotes() {
        return strictQuotes;
    }

    public boolean isIgnoreLeadingWhiteSpace() {
        return ignoreLeadingWhiteSpace;
    }

    public boolean isIgnoreQuotations() {
        return ignoreQuotations;
    }

    public NullFieldIndicator getNullFieldIndicator() {
        return nullFieldIndicator;
    }

    public CompressionType getCompression() {
        return compression;
    }

    public String getDatePattern() {
        return datePattern;
    }

    public String getTimePattern() {
        return timePattern;
    }

    public String getDatetimePattern() {
        return datetimePattern;
    }

    public String getTimestampPattern() {
        return timestampPattern;
    }
}
