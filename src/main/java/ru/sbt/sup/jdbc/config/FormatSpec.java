package ru.sbt.sup.jdbc.config;

import org.json.JSONObject;

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

    FormatSpec(JSONObject object) {
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

    JSONObject toJson() {
        return new JSONObject()
                .put("delimiter", "" + delimiter)
                .put("lineSeparator", lineSeparator)
                .put("quoteChar", "" + quoteChar)
                .put("escape", "" + escape)
                .put("commentChar", "" + commentChar)
                .put("header", header)
                .put("strictQuotes", strictQuotes)
                .put("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
                .put("ignoreQuotations", ignoreQuotations)
                .put("nullFieldIndicator", nullFieldIndicator.name().toLowerCase())
                .put("compression", compression.name().toLowerCase())
                .put("datePattern", datePattern)
                .put("timePattern", timePattern)
                .put("datetimePattern", datetimePattern)
                .put("timestampPattern", timestampPattern);
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
