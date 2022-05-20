package ru.sbt.sup.jdbc.config;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableSpec {

    public final String label;
    public final URI location;
    public FormatSpec format = null;
    public List<ColumnSpec> columns = null;

    public TableSpec(JsonObject object) {
        this.label = object.getString("label");
        this.location = URI.create(object.getString("location"));
        this.format = new FormatSpec(object.getJsonObject("format"));
        this.columns = object.getJsonArray("columns").stream()
                .map(v -> (JsonObject) v)
                .map(ColumnSpec::new)
                .collect(Collectors.toList());
    }

    public JsonObject toJson() {
        JsonArrayBuilder columnsJson = Json.createArrayBuilder();
        columns.stream().map(ColumnSpec::toJson).forEach(columnsJson::add);
        return Json.createObjectBuilder()
                .add("label", label)
                .add("location", location.toString())
                .add("format", format.toJson())
                .add("columns", columnsJson)
                .build();
    }

    public static List<TableSpec> generateTableSpecifications(String... keys) {
        List<TableSpec> builder = new ArrayList<>();
        for (String tableName : keys) {
            Path inputConfig = Paths.get("src", "test", "resources", tableName + ".json");
            try (JsonReader reader = Json.createReader(Files.newBufferedReader(inputConfig, StandardCharsets.UTF_8))) {
                JsonObject jsonObject = reader.readObject();
                TableSpec spec = new TableSpec(jsonObject);
                builder.add(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return builder;
    }

}
