package ru.sbt.sup.jdbc.config;

import org.json.JSONArray;
import org.json.JSONObject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSpec {

    private final String label;
    private final URI location;
    private FormatCSVSpec formatCSV = null;
    private FormatJsonSpec formatJson = null;
    private List<ColumnSpec> columns;

    public TableSpec(JSONObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        if (object.has(("formatCSV")))
            this.formatCSV = new FormatCSVSpec(object.getJSONObject("formatCSV"));
        if (object.has(("formatJson")))
            this.formatJson = new FormatJsonSpec(object.getJSONObject("formatJson"));
        this.columns = object.getJSONArray("columns").toList().stream()
                .map(v -> new JSONObject((Map)v))
                .map(ColumnSpec::new)
                .collect(Collectors.toList());
    }

    public JSONObject toJson() {
        JSONArray columnsJson = new JSONArray();
        columns.stream().map(ColumnSpec::toJson).forEach(columnsJson::put);
        JSONObject json = new JSONObject();
            json
                .put("label", label)
                .put("location", location.toString())
                .put("columns", columnsJson);
            if (formatCSV != null)
                json.put("formatCSV", formatCSV.toJson());
            if (formatJson != null)
                json.put("formatJson", formatJson.toJson());
            return json;
    }

    public FormatCSVSpec getCSVFormat() {
        return formatCSV;
    }

    public FormatJsonSpec getJsonFormat() {
        return formatJson;
    }

    public String getLabel() {
        return label;
    }

    public URI getLocation() {
        return location;
    }

    public List<ColumnSpec> getColumns() {
        return columns;
    }
}
