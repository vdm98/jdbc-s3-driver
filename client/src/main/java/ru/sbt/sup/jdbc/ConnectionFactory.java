package ru.sbt.sup.jdbc;

import org.json.JSONArray;
import org.json.JSONObject;
import ru.sbt.sup.jdbc.adapter.LakeSchemaFactory;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConnectionFactory {


    public static Connection getConnection() {
        List<TableSpec> tables = generateTableSpecifications("emps", "depts", "orders", "empsj");
        ConnSpec connSpec = getConnProperties();
        String schemaFactoryName = LakeSchemaFactory.class.getName();

        JSONArray tableSpecs = new JSONArray();
        tables.stream().map(TableSpec::toJson).forEach(tableSpecs::put);

        String model = new JSONObject()
            .put("version", "1.0")
            .put("defaultSchema", "default")
            .put("schemas", new JSONArray()
                .put(new JSONObject()
                    .put("name", "default")
                    .put("type", "custom")
                    .put("factory", schemaFactoryName)
                    .put("operand", new JSONObject()
                        .put("connSpec", connSpec.toJson())
                        .put("tableSpecs", tableSpecs)))).toString();


        try {
//            Path modelPath = Paths.get("src/main/resources/model.txt");
//            PrintWriter pw = new PrintWriter(modelPath.toFile());
//            pw.print(model);
//            pw.close();
//            BufferedReader in = new BufferedReader(new FileReader(modelPath.toFile()));
//            model = in.readLine(); // <-- read whole line
//            in.close();
//            String model = "{\"defaultSchema\":\"default\",\"schemas\":[{\"factory\":\"ru.sbt.sup.jdbc.adapter.LakeSchemaFactory\",\"name\":\"default\",\"type\":\"custom\",\"operand\":{\"tableSpecs\":[{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"firstname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"lastname\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"birthdate\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"salary\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"hiredate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"deptid\"}],\"format\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://222/emps.csv\",\"label\":\"emps\"},{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"deptname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"address\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"opendate\"}],\"format\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://222/depts.csv\",\"label\":\"depts\"},{\"columns\":[{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Region\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Country\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"ItemType\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"SalesChannel\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Priority\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"OrderDate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"OrderId\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"ShipDate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"UnitsSold\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"UnitPrice\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"UnitCost\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalRevenue\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalCost\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalProfit\"}],\"format\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd/MM/yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"M/d/yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://222/orders.csv\",\"label\":\"orders\"}],\"connSpec\":{\"secretKey\":\"+Rp8auCBwsKahsEDMtmfvTNELaz+pIjIYw5POwOs\",\"accessKey\":\"XCIA0YKIXGWUGYOKQ1V1\",\"endpointUrl\":\"http://127.0.0.1:9000\",\"region\":\"ru-1\"}}}],\"version\":\"1.0\"}";
//            String model = "{\"defaultSchema\":\"default\",\"schemas\":[{\"factory\":\"ru.sbt.sup.jdbc.adapter.LakeSchemaFactory\",\"name\":\"default\",\"type\":\"custom\",\"operand\":{\"tableSpecs\":[{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"firstname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"lastname\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"birthdate\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"salary\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"hiredate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"deptid\"}],\"format\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://222/emps.csv\",\"label\":\"emps\"}],\"connSpec\":{\"secretKey\":\"+Rp8auCBwsKahsEDMtmfvTNELaz+pIjIYw5POwOs\",\"accessKey\":\"XCIA0YKIXGWUGYOKQ1V1\",\"endpointUrl\":\"http://10.31.5.89:9000\",\"region\":\"ru-1\"}}}],\"version\":\"1.0\"}";
//            System.out.println(model);
            return DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
        } catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }

    public static List<TableSpec> generateTableSpecifications(String... keys) {
        List<TableSpec> builder = new ArrayList<>();
        for (String tableName : keys) {
            Path inputConfig = Paths.get("..", "src", "test", "resources", tableName + ".json");
            try {
                String content = new String(Files.readAllBytes(inputConfig));
                JSONObject jsonObject = new JSONObject(content);
                TableSpec spec = new TableSpec(jsonObject);
                builder.add(spec);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return builder;
    }

    private static ConnSpec getConnProperties() {
        Properties appProps = new Properties();
        Path inputConfig = Paths.get("..", "src", "test", "resources", "application.properties");
        try {
            appProps.load(Files.newInputStream(inputConfig.toAbsolutePath()));
        } catch (IOException ex){
            throw new RuntimeException(ex);
        }
        return new ConnSpec(
                appProps.getProperty("accessKey"),
                appProps.getProperty("secretKey"),
                appProps.getProperty("endpointUrl"),
                appProps.getProperty("region"));
    }
}
