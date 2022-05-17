package ru.sbt.sup.jdbc;

import ru.sbt.sup.jdbc.adapter.LakeSchemaFactory;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class LakeDriver {
    static {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection(ConnSpec connSpec, List<TableSpec> tables) throws SQLException {
        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
        tables.stream().map(TableSpec::toJson).forEach(jsonArrayBuilder::add);
        JsonArray build = jsonArrayBuilder.build();
        String tableSpecs = build.toString();
        String schemaFactoryName = LakeSchemaFactory.class.getName();
        JsonObject modelJson = Json.createObjectBuilder()
                .add("version", "1.0")
                .add("defaultSchema", "default")
                .add("schemas", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("name", "default")
                                .add("type", "custom")
                                .add("factory", schemaFactoryName)
                                .add("operand", Json.createObjectBuilder()
                                        .add("connSpec", connSpec.toJson().toString())
                                        .add("inputs", tableSpecs))))
                .build();
        return DriverManager.getConnection("jdbc:calcite:model=inline:" + modelJson);
    }
}
