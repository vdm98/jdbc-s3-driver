package ru.sbt.sup.jdbc.config;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSpec {

    public final String label;
    public final URI location;
    public FormatSpec format = null;
    public List<ColumnSpec> columns = null;

    public TableSpec(JSONObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        this.format = new FormatSpec(object.getJSONObject("format"));
        this.columns = object.getJSONArray("columns").toList().stream()
                .map(v -> new JSONObject((Map)v))
                .map(ColumnSpec::new)
                .collect(Collectors.toList());
    }

    public JSONObject toJson() {
        JSONArray columnsJson = new JSONArray();
        columns.stream().map(ColumnSpec::toJson).forEach(columnsJson::put);
        return new JSONObject()
                .put("label", label)
                .put("location", location.toString())
                .put("format", format.toJson())
                .put("columns", columnsJson);
    }

}
