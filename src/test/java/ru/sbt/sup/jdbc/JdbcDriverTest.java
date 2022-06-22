package ru.sbt.sup.jdbc;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.sbt.sup.jdbc.adapter.LakeSchemaFactory;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcDriverTest {

    private static final List<TableSpec> tables = generateTableSpecifications("emps", "depts", "orders", "empsj");

    private static Stream<Arguments> inputProvider() {
        return Stream.of(
                // 1. CSV data. Simple filter by primary key.
                Arguments.of(
                    "select id, firstname, lastname from emps where id=1",
                    // EXPECTED RESULT
                    "1,aaa,AAA\n"
                ),
                // 2. CSV data. Filter with ORs.
                Arguments.of(
                    "select id, firstname, lastname from emps where id=1 or firstname='bbb' or firstname='ccc'",
                    // EXPECTED RESULT
                    "1,aaa,AAA\n2,bbb,BBB\n3,ccc,CCC\n"
                ),
                // 3. CSV data. IN operator.
                Arguments.of(
                    "select * from emps where id in (2,3)",
                    // EXPECTED RESULT
                    "2,bbb,BBB,1992-01-02,2000.0,2020-02-02,10\n3,ccc,CCC,1993-01-03,3000.0,2020-03-03,20\n"
                ),
                // 4. CSV data. AND, OR filters with PARENTHESES.
                Arguments.of(
                    "select id, firstname, lastname from emps where (firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3",
                    // EXPECTED RESULT
                    "3,ccc,CCC\n4,ddd,DDD\n"
                ),
                // 5. CSV data. JOIN tables, IN operator, DATE filter.
                Arguments.of(
                    "select e.id, e.lastname, e.hiredate, d.deptname from emps e inner join depts d on e.deptid = d.id " +
                    "where d.id in (10,20,30) and e.hiredate > CAST('2020-03-01' AS DATE)",
                    // EXPECTED RESULT
                    "3,CCC,2020-03-03,dept2\n4,DDD,2020-04-04,dept2\n5,EEE,2020-05-05,dept3\n6,FFF,2020-06-06,dept3\n"
                ),
                // 6. CSV data. DATE range filter.
                Arguments.of(
                    "select o.orderid, o.country, o.shipdate from orders o where o.shipdate > CAST('2012-01-20' AS DATE) and o.shipdate <= CAST('2014-07-05' AS DATE)",
                    // EXPECTED RESULT
                    "963881480,Grenada,2012-09-15\n" +
                    "341417157,Russia,2014-05-08\n" +
                    "514321792,Sao Tome and Principe,2014-07-05\n" +
                    "115456712,Rwanda,2013-02-06\n" +
                    "871543967,Burkina Faso,2012-07-27\n"
                ),
                // 7. CSV data. BETWEEN two decimals OR BETWEEN two dates.
                Arguments.of(
                    "select id, lastname, salary, hiredate from emps " +
                    "where salary between 3500.0 and 4500.0 " +
                    "or hiredate between CAST('2020-03-01' AS DATE) and CAST('2020-05-01' AS DATE)",
                    // EXPECTED RESULT
                    "3,CCC,3000.0,2020-03-03\n" +
                    "4,DDD,3000.0,2020-04-04\n" +
                    "7,GGG,4000.0,2020-07-07\n" +
                    "8,HHH,4000.0,2020-08-08\n"
                ),
                // 8. CSV data. JOIN tables, GROUP BY, SUM operators.
                Arguments.of(
                    "select sum(e.salary), d.deptname " +
                    "from emps e inner join depts d on e.deptid = d.id " +
                    "where d.id in (10,20,30) " +
                    "group by d.deptname",
                    // EXPECTED RESULT
                    "3000.0,dept1\n6000.0,dept2\n3000.0,dept3\n"
                ),
                // 9. Json data. Simple filter by primary key.
                Arguments.of(
                        "select id, firstname, lastname from empsj where id=1",
                        // EXPECTED RESULT
                        "1,aaa,AAA\n"
                ),
                // 10. Json data. Filter with ORs.
                Arguments.of(
                        "select id, firstname, lastname from empsj where id=1 or firstname='bbb' or firstname='ccc'",
                        // EXPECTED RESULT
                        "1,aaa,AAA\n2,bbb,BBB\n3,ccc,CCC\n"
                ),
                // 11. Json data. IN operator
                Arguments.of(
                        "select * from empsj where id in (2,3)",
                        // EXPECTED RESULT
                        "2,bbb,BBB,1992-01-02,2000.0,2020-02-02,10\n3,ccc,CCC,1993-01-03,3000.0,2020-03-03,20\n"
                ),
                // 12. Json data. AND, OR filters with PARENTHESES.
                Arguments.of(
                        "select id, firstname, lastname from empsj where (firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3",
                        // EXPECTED RESULT
                        "3,ccc,CCC\n4,ddd,DDD\n"
                ),
                // 13. Json & CSV data. JOIN tables, IN operator, DATE filter.
                Arguments.of(
                        "select e.id, e.lastname, e.hiredate, d.deptname from empsj e inner join depts d on e.deptid = d.id " +
                                "where d.id in (10,20,30) and e.hiredate > CAST('2020-03-01' AS DATE)",
                        // EXPECTED RESULT
                        "3,CCC,2020-03-03,dept2\n4,DDD,2020-04-04,dept2\n5,EEE,2020-05-05,dept3\n6,FFF,2020-06-06,dept3\n"
                ),
                // 14. Json data. BETWEEN two decimals OR BETWEEN two dates.
                Arguments.of(
                        "select id, lastname, salary, hiredate from empsj " +
                        "where salary between 3500.0 and 4500.0 " +
                        "or hiredate between CAST('2020-03-01' AS DATE) and CAST('2020-05-01' AS DATE)",
                        // EXPECTED RESULT
                        "3,CCC,3000.0,2020-03-03\n" +
                        "4,DDD,3000.0,2020-04-04\n" +
                        "7,GGG,4000.0,2020-07-07\n" +
                        "8,HHH,4000.0,2020-08-08\n"
                ),
                // 15. Json & CSV data. JOIN tables, GROUP BY, SUM operators.
                Arguments.of(
                        "select sum(e.salary), d.deptname " +
                        "from empsj e inner join depts d on e.deptid = d.id " +
                        "where d.id in (10,20,30) " +
                        "group by d.deptname",
                        // EXPECTED RESULT
                        "3000.0,dept1\n6000.0,dept2\n3000.0,dept3\n"
                ));
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3(String query,String result) throws IOException {
        String resultString = executeTask(query);
        assertEquals(result, resultString);
    }

    private static String executeTask(String query) throws IOException {
        StringBuffer result = new StringBuffer();
        try (Connection connection = getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                ResultSetMetaData metaData = statement.getMetaData();
                int limit = metaData.getColumnCount();
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        List<String> builder = new ArrayList<>();
                        for (int i = 1; i <= limit; i++) {
                            String value;
                            if (metaData.getColumnType(i) == Types.TIMESTAMP){
                                Calendar tzCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                                value = resultSet.getDate(i, tzCal).toString();
                            } else {
                                value = resultSet.getString(i);
                            }
                            builder.add(value);
                        }
                        result.append(String.join(",", builder)).append("\n");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result.toString();
    }

    public static Connection getConnection() throws SQLException {
        ConnSpec connSpec = getConnProperties();
        String schemaFactoryName = LakeSchemaFactory.class.getName();

        JSONArray tableSpecs = new JSONArray();
        tables.stream().map(TableSpec::toJson).forEach(tableSpecs::put);

        JSONObject model = new JSONObject()
            .put("version", "1.0")
            .put("defaultSchema", "default")
            .put("schemas", new JSONArray()
                .put(new JSONObject()
                    .put("name", "default")
                    .put("type", "custom")
                    .put("factory", schemaFactoryName)
                    .put("operand", new JSONObject()
                        .put("connSpec", connSpec.toJson())
                        .put("tableSpecs", tableSpecs))));

        return DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
    }

    public static List<TableSpec> generateTableSpecifications(String... keys) {
        List<TableSpec> builder = new ArrayList<>();
        for (String tableName : keys) {
            Path inputConfig = Paths.get("src", "test", "resources", tableName + ".json");
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
        Path inputConfig = Paths.get("src", "test", "resources", "application.properties");
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