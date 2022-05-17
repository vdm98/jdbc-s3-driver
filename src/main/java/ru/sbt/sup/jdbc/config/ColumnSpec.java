package ru.sbt.sup.jdbc.config;

import javax.json.Json;
import javax.json.JsonObject;

public class ColumnSpec {

    public final String label;
    public final TypeSpec datatype;
    public final Boolean nullable;

    ColumnSpec(JsonObject object) {
        this.label = object.getString("label");
        this.datatype = TypeSpec.of(object.getString("datatype"));
        this.nullable = object.getBoolean("nullable");
    }

    JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("label", label)
                .add("datatype", datatype.toJson())
                .add("nullable", nullable)
                .build();
    }

}
