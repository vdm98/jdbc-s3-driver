package ru.sbt.sup.jdbc;

import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client {

    private static final Logger logger = LogManager.getLogger(Client.class);

    private static String sqlScript =
            //"select o.orderid, o.country, p.name from orders o inner join priority p on o.priority=p.code " +
            //        "where o.shipdate > CAST('2014-01-01' AS DATE)";
            //"select o.orderid, o.country, o.shipdate from orders o where o.shipdate > CAST('2012-01-20' AS DATE) and o.shipdate <= CAST('2014-07-05' AS DATE)";
            //"select * from emps e inner join depts d on d.id=e.deptid where d.name='Sales'";//" where id=1";
            //"select * from orders where country in ('Russia', 'Mexico', 'Australia') order by country";
            //"where country in ('Russia', 'Mexico', 'Australia') and p.name='Low'";
            "select id, firstname from emps";
            //" or hiredate between CAST('2020-09-01' AS DATE) and CAST('2020-11-01' AS DATE)";
            //"select id, firstname, lastname from people where id=1 or firstname='bbb' or firstname='ccc' order by id desc";
             //"select id, firstname, lastname from people where (not firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3";
             //"select * from emps where id=1";
//            "select id, firstname, lastname from people where id=1 or firstname in ('aa','bb')";
//            "select id, firstname, lastname from people where id=1 and firstname='aa'";
//            "select * from depts";// or firstname='bbb' or lastname='CCC'";
//            "select id, firstname, lastname from people";// where firstname='bb'";
 //           "select * from people where not id > 2";
//            "select max(relationships.subject_person_id) from relationships";
//            "select sum(people.id) from people";
//            "select avg(people.id) from people";

    public static void main(String[] args) throws IOException {
        int l = 0;
        StringBuffer result = new StringBuffer();
        try (Connection connection = ConnectionFactory.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(sqlScript)) {
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
                        result.append(++l).append(". ").append(String.join(", ", builder)).append("\n");
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("\n*** RESULT ***\n" + result);
    }


}
