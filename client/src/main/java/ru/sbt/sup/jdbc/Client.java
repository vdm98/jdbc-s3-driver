package ru.sbt.sup.jdbc;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client {

    private static final Logger logger = LogManager.getLogger(Client.class);

    private static String sqlScript = "select lastname, id from empsj where id=2";


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
