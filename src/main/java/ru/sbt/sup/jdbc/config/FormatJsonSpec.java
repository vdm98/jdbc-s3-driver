package ru.sbt.sup.jdbc.config;

import org.json.JSONObject;

public class FormatJsonSpec {

    private final CompressionType compression;
    private final String fromClause;

    FormatJsonSpec(JSONObject object) {
        this.fromClause = object.getString("fromClause");
        this.compression = CompressionType.valueOf(object.getString("compression").toUpperCase());
    }

    public enum CompressionType {
        NONE,
        GZIP
    }

    public CompressionType getCompression() {
        return compression;
    }

    public String getFromClause() {
        return fromClause;
    }

    JSONObject toJson() {
        return new JSONObject()
                .put("fromClause", "" + fromClause)
                .put("compression", compression.name().toLowerCase());
    }
}