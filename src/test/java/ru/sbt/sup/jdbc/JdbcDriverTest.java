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

    private static final List<TableSpec> tableSpecs = TableSpec.generateTableSpecifications("emps", "depts", "orders");

    private static Stream<Arguments> inputProvider() {
        return Stream.of(
                // Simple filter by primary key
                Arguments.of(
                    "select id, firstname, lastname from emps where id=1",
                    // EXPECTED RESULT
                    "1,aaa,AAA\n"
                ),
                // Filter with ORs
                Arguments.of(
                    "select id, firstname, lastname from emps where id=1 or firstname='bbb' or firstname='ccc'",
                    // EXPECTED RESULT
                    "1,aaa,AAA\n2,bbb,BBB\n3,ccc,CCC\n"
                ),
                // IN operator
                Arguments.of(
                    "select * from emps where id in (2,3)",
                    // EXPECTED RESULT
                    "2,bbb,BBB,1992-01-02,1000.22,2020-02-02,10\n3,ccc,CCC,1993-01-02,1000.33,2020-03-03,20\n"
                ),
                // AND with ORs filter
                Arguments.of(
                    "select id, firstname, lastname from emps where (firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3",
                    // EXPECTED RESULT
                    "3,ccc,CCC\n4,ddd,DDD\n"
                ),
                // JOIN tables, IN operator, DATE filter
                Arguments.of(
                    "select e.id, e.lastname, e.hiredate, d.deptname from emps e inner join depts d on e.deptid = d.id " +
                    "where d.id in (10,20,30) and e.hiredate > CAST('2020-03-01' AS DATE)",
                    // EXPECTED RESULT
                    "3,CCC,2020-03-03,dept2\n4,DDD,2020-04-04,dept2\n5,EEE,2020-05-05,dept3\n6,FFF,2020-06-06,dept3\n"
                ),
                // DATE range filter
                Arguments.of(
                    "select o.orderid, o.country, o.shipdate from orders o where o.shipdate > CAST('2012-01-20' AS DATE) and o.shipdate <= CAST('2014-07-05' AS DATE)",
                    // EXPECTED RESULT
                    "963881480,Grenada,2012-09-15\n" +
                    "341417157,Russia,2014-05-08\n" +
                    "115456712,Rwanda,2013-02-06\n" +
                    "871543967,Burkina Faso,2012-07-27\n"
                ),
                // JOIN tables, GROUP BY, SUM
                Arguments.of(
                    "select sum(e.salary), d.deptname " +
                    "from emps e inner join depts d on e.deptid = d.id " +
                    "where d.id in (10,20,30) " +
                    "group by d.deptname",
                    // EXPECTED RESULT
                    "2000.33,dept1\n2000.77,dept2\n2001.21,dept3\n"
                ));
    }

    @ParameterizedTest
    @MethodSource("inputProvider")
    void testS3(String query,String result) throws IOException {
        String resultString = executeTask(query);
        assertEquals(result, resultString);
    }

    private static String executeTask(String query) throws IOException {
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