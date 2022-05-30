package ru.sbt.sup.jdbc.adapter;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import ru.sbt.sup.jdbc.config.ConnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;

//import javax.json.Json;
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import javax.json.JsonReader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchemaFactory implements SchemaFactory {

    public static final LakeSchemaFactory INSTANCE = new LakeSchemaFactory();
    public LakeSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {

        return new AbstractSchema() {
            private Map<String, Table> tableMap = null;
            @Override
            public boolean isMutable() {
                return false;
            }

            @Override
            protected Map<String, Table> getTableMap() {
                // workaround to prevent multiple scanner class creation
                if (tableMap != null) return tableMap;
                ConnSpec connSpec = extractConnOperand(operand);
                JSONArray inputTables = extractTableSpecsOperand(operand);

                BasicAWSCredentials awsCreds = new BasicAWSCredentials(connSpec.getAccessKey(), connSpec.getSecretKey());
                AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(connSpec.getEndpointUrl(), connSpec.getRegion());
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                        .withEndpointConfiguration(endpoint)
                        .withPathStyleAccessEnabled(true)
                        .build();

                tableMap = inputTables.toList().stream()
                        .map(v -> new JSONObject((Map)v))
                        .map(TableSpec::new)
                        .collect(Collectors.toMap(
                                spec -> spec.label.toUpperCase(),
                                spec -> new LakeTable(s3Client,  spec)));
                return tableMap;
            }
        };
    }

    private JSONArray extractTableSpecsOperand(Map<String, Object> operand) {
        return new JSONArray((List)operand.get("tableSpecs"));
    }

    private ConnSpec extractConnOperand(Map<String, Object> operand) {
        JSONObject value = new JSONObject((Map)operand.get("connSpec"));
        return ConnSpec.fromJson(value);
    }
}
