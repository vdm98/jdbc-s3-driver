package ru.sbt.sup.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

    private static Stream<Arguments> inputProvider() {
        List<TableSpec> peopleTableSpec = TableSpec.generateTableSpecifications("people");
        List<TableSpec> ordersTableSpec = TableSpec.generateTableSpecifications("orders");
        return Stream.of(
//                Arguments.of(
//                        "select id, firstname, lastname from people where id=1",
//                        peopleTableSpec,
//                        "1,aaa,AAA\n"
//                ),
//                Arguments.of(
//                        "select id, firstname, lastname from people where id=1 or firstname='bbb' or firstname='ccc'",
//                        peopleTableSpec,
//                        "1,aaa,AAA\n2,bbb,BBB\n3,ccc,CCC\n"
//                ),
//                Arguments.of(
//                        "select * from people where id in (2,3)",
//                        peopleTableSpec,
//                        "2,bbb,BBB\n3,ccc,CCC\n"
//                ),
//                Arguments.of(
//                        "select id, firstname, lastname from people where (firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3",
//                        peopleTableSpec,
//                        "3,ccc,CCC\n4,ddd,DDD\n"
//                ),
                Arguments.of(
                        "select o.orderid, o.country, o.shipdate from orders o where o.shipdate > CAST('2012-01-20' AS DATE) and o.shipdate <= CAST('2014-07-05' AS DATE)",
                        ordersTableSpec,
                        "963881480,Grenada,2012-09-15\n" +
                        "341417157,Russia,2014-05-08\n" +
                        "115456712,Rwanda,2013-02-06\n" +
                        "871543967,Burkina Faso,2012-07-27\n"
                ));
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3(String query, List<TableSpec> schema, String result) throws IOException {
        String resultString = executeTaskAndGetResult(schema, query);
        assertEquals(result, resultString);
    }

    private static String executeTaskAndGetResult(List<TableSpec> tableSpecs, String query) throws IOException {
        ConnSpec connSpec = getConnProperties();

        StringBuffer result = new StringBuffer();
        try (Connection connection = LakeDriver.getConnection(connSpec, tableSpecs)) {
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

    private static ConnSpec getConnProperties() throws IOException {
        Properties appProps = new Properties();
        Path inputConfig = Paths.get("src/main/resources/application.properties");
        appProps.load(Files.newInputStream(inputConfig.toAbsolutePath()));
        return new ConnSpec(
                appProps.getProperty("accessKey"),
                appProps.getProperty("secretKey"),
                appProps.getProperty("endpointUrl"),
                appProps.getProperty("region"));
    }
}