package ru.sbt.sup.jdbc;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class);
    private static StringBuffer result = new StringBuffer();

    public static void main(String[] args) {

//        String sql = /*"explain plan for*/ "select country, max(totalprofit) from orders /*where priority='H'*/ group by country";//" order by country";
        String sql = /*"explain plan for*/ "select * from emps where id between 3 and 7";//" order by country";


//        String sql = "select * from emps where id in (5,7)";
//        String sql = "select * from emps where hiredate >= CAST(? AS DATE)";
//        String sql = "select * from emps where hiredate >= ?";
//        String sql = "select * from emps where hiredate >= CAST('2020-03-04' AS DATE)";

//        String model = "{\"defaultSchema\":\"default\",\"schemas\":[{\"factory\":\"ru.sbt.sup.jdbc.adapter.LakeSchemaFactory\",\"name\":\"default\",\"type\":\"custom\",\"operand\":{\"tableSpecs\":[{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"firstname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"lastname\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"birthdate\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"salary\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"hiredate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"deptid\"}],\"formatCSV\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://vadimakh/emps.csv\",\"label\":\"emps\"},{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"deptname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"address\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"opendate\"}],\"formatCSV\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://vadimakh/depts.csv\",\"label\":\"depts\"},{\"columns\":[{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Region\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Country\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"ItemType\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"SalesChannel\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"Priority\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"OrderDate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"OrderId\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"ShipDate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"UnitsSold\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"UnitPrice\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"UnitCost\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalRevenue\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalCost\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"TotalProfit\"}],\"formatCSV\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd/MM/yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"M/d/yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"location\":\"s3://vadimakh/orders.csv\",\"label\":\"orders\"},{\"columns\":[{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"id\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"firstname\"},{\"nullable\":false,\"datatype\":\"string\",\"label\":\"lastname\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"birthdate\"},{\"nullable\":false,\"datatype\":\"double\",\"label\":\"salary\"},{\"nullable\":false,\"datatype\":\"date\",\"label\":\"hiredate\"},{\"nullable\":false,\"datatype\":\"integer\",\"label\":\"deptid\"}],\"formatCSV\":{\"quoteChar\":\"\\\"\",\"timePattern\":\"HH:mm:ss\",\"timestampPattern\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\",\"datetimePattern\":\"dd.MM.yyyy HH:mm:ss\",\"lineSeparator\":\"\\n\",\"commentChar\":\"#\",\"nullFieldIndicator\":\"neither\",\"ignoreLeadingWhiteSpace\":false,\"delimiter\":\",\",\"datePattern\":\"dd.MM.yyyy\",\"strictQuotes\":false,\"ignoreQuotations\":false,\"header\":false,\"compression\":\"none\",\"escape\":\"\\\\\"},\"formatJson\":{\"fromClause\":\"S3Object[*][*]\",\"compression\":\"none\"},\"location\":\"s3://vadimakh/emps.json\",\"label\":\"empsj\"}],\"connSpec\":{\"secretKey\":\"+Rp8auCBwsKahsEDMtmfvTNELaz+pIjIYw5POwOs\",\"accessKey\":\"XCIA0YKIXGWUGYOKQ1V1\",\"endpointUrl\":\"http://127.0.0.1:9000\",\"region\":\"ru-1\"}}}],\"version\":\"1.0\"}";
//         try (Connection con = DriverManager.getConnection("jdbc:calcite:model=inline:" + model)) {

        try (Connection con = ConnectionFactory.getConnection()) {
            try (PreparedStatement statement = con.prepareStatement(sql)) {
                ResultSetMetaData metaData = statement.getMetaData();
                int cols = metaData.getColumnCount();
//                statement.setLong(1, Date.valueOf("2020-03-04").getTime());
//                statement.setDate(1, Date.valueOf("2020-03-04"));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        List<String> builder = new ArrayList<>();
                        for (int i = 1; i <= cols; i++) {
                            String value;
                            if (metaData.getColumnType(i) == Types.TIMESTAMP){
                                Calendar tzCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                                value = resultSet.getDate(i, tzCal).toString();
                            } else {
                                value = resultSet.getString(i);
                            }
                            builder.add(value);
                        }
                        result.append(String.join(", ", builder)).append("\n");
                    }
                }
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("\n*** RESULT ***\n" + result);
    }
}