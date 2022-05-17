package ru.sbt.sup.jdbc;

import ru.sbt.sup.jdbc.config.TableSpec;
import ru.sbt.sup.jdbc.adapter.LakeS3Adapter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Client {

    private static String query =

//            "select people.id, relationships.object_person_id from people " +
//            "inner join relationships on people.id = relationships.subject_person_id " +
//            "and (relationships.predicate_id = 'has_friend' " +
//            "and people.firstname like 'aa%')";

//            "select id, firstname, lastname from people where id=1 and (firstname='aa' or firstname='bb')";
//            "select id, firstname, lastname from people where id=1 or firstname in ('aa','bb')";

//            "select id, firstname, lastname from people where id=1 and firstname='aa'";

          //  "select id, firstname, lastname from people where id in (1,5)";// or firstname='bbb' or lastname='CCC'";
//            "select id, firstname, lastname from people where firstname='bb'";
                "select * from people where not id > 2";


//            "select max(relationships.subject_person_id) from relationships";
//            "select sum(people.id) from people";
//            "select avg(people.id) from people";


    public static void main(String[] args) {
        List<TableSpec> specs = TableSpec.generateTableSpecifications("people");//, "relationships");
        String result = getResult(specs, query);
        System.out.println("\n*** RESULT ***\n" + result);
    }

    private static String getResult(List<TableSpec> tableSpecs, String sqlScript) {
        StringBuffer result = new StringBuffer();
        try (Connection connection = LakeDriver.getConnection(tableSpecs, LakeS3Adapter.class)) {
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
        return result.toString();
    }
}
