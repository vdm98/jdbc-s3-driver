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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcDriverTest {

    private static Stream<Arguments> inputProvider() {
        return Stream.of(
                Arguments.of(
                        "select id, firstname, lastname from people where id=1",
                        TableSpec.generateTableSpecifications("people"),
                        "1,aaa,AAA\n"
                ),
                Arguments.of(
                        "select id, firstname, lastname from people where id=1 or firstname='bbb' or firstname='ccc'",
                        TableSpec.generateTableSpecifications("people"),
                        "1,aaa,AAA\n2,bbb,BBB\n3,ccc,CCC\n"
                ),
                Arguments.of(
                        "select * from people where id in (2,3)",
                        TableSpec.generateTableSpecifications("people"),
                        "2,bbb,BBB\n3,ccc,CCC\n"
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
                            String string = resultSet.getString(i);
                            builder.add(string);
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