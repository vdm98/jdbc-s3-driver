package ru.sbt.sup.jdbc.config;

import javax.json.Json;
import javax.json.JsonObject;

public class ConnSpec {
    private String accessKey;
    private String secretKey;
    private String endpointUrl;
    private String region;

    public ConnSpec(String accessKey, String secretKey, String endpointUrl, String region) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpointUrl = endpointUrl;
        this.region = region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public String getRegion() {
        return region;
    }

    public JsonObject toJson() {
        return Json.createObjectBuilder()
                .add("accessKey", accessKey)
                .add("secretKey", secretKey)
                .add("endpointUrl", endpointUrl)
                .add("region", region)
                .build();
    }

    public static ConnSpec fromJson(JsonObject object) {
        return new ConnSpec(
                object.getString("accessKey"),
                object.getString("secretKey"),
                object.getString("endpointUrl"),
                object.getString("region"));
    }
}
