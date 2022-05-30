package ru.sbt.sup.jdbc.config;

import org.json.JSONObject;

public class ColumnSpec {

    public final String label;
    public final TypeSpec datatype;
    public final Boolean nullable;

    public ColumnSpec(JSONObject object) {
        this.label = object.getString("label");
        this.datatype = TypeSpec.of(object.getString("datatype"));
        this.nullable = object.getBoolean("nullable");
    }

    JSONObject toJson() {
        return new JSONObject()
                .put("label", label)
                .put("datatype", datatype.toJson())
                .put("nullable", nullable);
    }
}
