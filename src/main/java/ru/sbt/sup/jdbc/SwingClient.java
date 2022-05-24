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

    public static void main(String[] args) throws IOException {
        Result result = executeQuery("select id, lastname, firstname, salary, hiredate from emps"); // where id in (1,3,5,7,9)");

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
        scrollPane.setBounds(10, 150, 1150, 400);

        JTextArea queryTextArea  = new JTextArea();
        queryTextArea.setText("select id, lastname, firstname, salary, hiredate from emps");
        queryTextArea.setBounds(10, 10, 1000, 130);

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

        frame.setContentPane(panel);
        frame.setSize(1168, 583);
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
