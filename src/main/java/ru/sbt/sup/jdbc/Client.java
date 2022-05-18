package ru.sbt.sup.jdbc;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client {

    private static final Logger logger = LogManager.getLogger(Client.class);

    private static String sqlScript =
//            "select people.id, relationships.object_person_id from people " +
//            "inner join relationships on people.id = relationships.subject_person_id " +
//            "and (relationships.predicate_id = 'has_friend' " +
//            "and people.firstname like 'aa%')";

            //"select id, firstname, lastname from people where id=1 or firstname='aaa' or firstname='bbb'";

             "select id, firstname, lastname from people where (not firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3";
             //"select * from emps where id=1";

//            "select id, firstname, lastname from people where id=1 or firstname in ('aa','bb')";
//            "select id, firstname, lastname from people where id=1 and firstname='aa'";
//            "select id, firstname, lastname from people where id in (1,5)";// or firstname='bbb' or lastname='CCC'";
//           "select id, firstname, lastname from people";// where firstname='bb'";
 //             "select * from people where not id > 2";
//            "select max(relationships.subject_person_id) from relationships";
//            "select sum(people.id) from people";
//            "select avg(people.id) from people";

    public static void main(String[] args) throws IOException {
        List<TableSpec> tableSpecs = TableSpec.generateTableSpecifications("people");//, "relationships");
        ConnSpec connSpec = getConnProperties();

        StringBuffer result = new StringBuffer();
        try (Connection connection = LakeDriver.getConnection(connSpec, tableSpecs)) {
            try (PreparedStatement statement = connection.prepareStatement(sqlScript)) {
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
        logger.info("\n*** RESULT ***\n" + result);
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
