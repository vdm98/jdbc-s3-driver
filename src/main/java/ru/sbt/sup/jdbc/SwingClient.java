package ru.sbt.sup.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.List;

public class SwingClient {

    private static final String[] queries = {
            "select * from orders order by country",
            "select country, count(*) \"COUNT\", sum(TotalProfit) \"TOTAL\" from orders group by country order by sum(TotalProfit) desc",
            "select id, firstname, lastname from emps where id=1",
            "select id, firstname, lastname from emps where id=1 or firstname='bbb' or firstname='ccc'",
            "select * from emps where id in (2,3)",
            "select id, firstname, lastname from emps where (firstname like 'b%' or firstname='ccc' or firstname='ddd') and id>=3",
            "select e.id, e.lastname, e.hiredate, d.deptname from emps e inner join depts d on e.deptid = d.id where d.id in (10,20,30) and e.hiredate > CAST('2020-03-01' AS DATE)",
            "select o.orderid, o.country, o.shipdate from orders o where o.shipdate > CAST('2012-01-20' AS DATE) and o.shipdate <= CAST('2014-07-05' AS DATE)",
            "select id, lastname, salary, hiredate from emps where salary between 1000.25 and 1000.65 or hiredate between CAST('2020-09-01' AS DATE) and CAST('2020-11-01' AS DATE)",
            "select sum(e.salary), d.deptname from emps e inner join depts d on e.deptid = d.id where d.id in (10,20,30) group by d.deptname"};

    public static void main(String[] args) throws IOException {
        Result result = executeQuery(queries[0]);

        JFrame frame = new JFrame();
        JPanel panel = new JPanel();
        panel.setLayout(null);  //new BoxLayout(panel, BoxLayout.Y_AXIS));

        DefaultTableModel tableModel = new DefaultTableModel(result.data, result.columns);
        JTable resultTable = new JTable(tableModel);
        //resultTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        resultTable.getTableHeader().setOpaque(false);
        resultTable.getTableHeader().setBackground(new Color(0, 0, 150));
        resultTable.getTableHeader().setForeground(Color.WHITE);
        resultTable.getTableHeader().setFont(new Font("Arial", Font.BOLD, 13));

        JScrollPane scrollPane = new JScrollPane(resultTable);
        scrollPane.setBounds(10, 180, 1150, 400);

        JTextArea queryTextArea  = new JTextArea();
        queryTextArea.setText(queries[0]);
        queryTextArea.setBounds(10, 10, 1000, 130);

        JComboBox querySetCombobox = new JComboBox(queries);
        querySetCombobox.setBounds(5, 150, 1162, 20);
        querySetCombobox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JComboBox box = (JComboBox)e.getSource();
                String item = (String)box.getSelectedItem();
                queryTextArea.setText(item);
            }
        });

        JButton runButton = new JButton("RUN");
        runButton.setBounds(1020, 7, 140, 135);
        runButton.setBackground(Color.GRAY);
        runButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    Result result = executeQuery(queryTextArea.getText());
                    DefaultTableModel tableModel1 = new DefaultTableModel(result.data, result.columns);
                    resultTable.setModel(tableModel1);
                } catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        });

        Path inputConfig = Paths.get("src/main/resources/run.png");
        ImageIcon icon = new ImageIcon(inputConfig.toString());
        Image img = icon.getImage() ;
        Image newimg = img.getScaledInstance( 30, 30,  java.awt.Image.SCALE_SMOOTH ) ;
        icon = new ImageIcon( newimg );
        runButton.setIcon(icon);

        panel.add(queryTextArea);
        panel.add(scrollPane);
        panel.add(runButton);
        panel.add(querySetCombobox);

        frame.setContentPane(panel);
        frame.setSize(1168, 683);
        frame.setResizable(true);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    private static Result executeQuery(String query) throws IOException {
        List<TableSpec> tableSpecs = TableSpec.generateTableSpecifications("emps", "depts", "orders");
        ConnSpec connSpec = getConnProperties();
        ArrayList<String[]> rows = new ArrayList<>();
        Result result = new Result();
        try (Connection connection = LakeDriver.getConnection(connSpec, tableSpecs)) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                ResultSetMetaData metaData = statement.getMetaData();
                result.columns = getColumns(metaData);
                int limit = metaData.getColumnCount();
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String[] cols = new String[limit];
                        for (int i = 1; i <= limit; i++) {
                            String value;
                            if (metaData.getColumnType(i) == Types.TIMESTAMP) {
                                Calendar tzCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                                value = resultSet.getDate(i, tzCal).toString();
                            } else {
                                value = resultSet.getString(i);
                            }
                            cols[i-1] = value;
                        }
                        rows.add(cols);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        result.data = rows.stream().toArray(String[][]::new);
        return result;
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

    private static class Result {
        String[][] data;
        String[] columns;
    }

    private static String[] getColumns(ResultSetMetaData metaData) throws SQLException {
        int limit = metaData.getColumnCount();
        String[] cols = new String[limit];
        for (int i = 1; i <= limit; i++) {
            cols[i-1] = metaData.getColumnName(i);
        }
        return cols;
    }
}
