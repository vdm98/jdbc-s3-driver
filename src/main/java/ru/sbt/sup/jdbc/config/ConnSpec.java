package ru.sbt.sup.jdbc.config;

import org.json.JSONObject;

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

    public JSONObject toJson() {
        return new JSONObject()
                .put("accessKey", accessKey)
                .put("secretKey", secretKey)
                .put("endpointUrl", endpointUrl)
                .put("region", region);
    }

    public static ConnSpec fromJson(JSONObject object) {
        return new ConnSpec(
                object.getString("accessKey"),
                object.getString("secretKey"),
                object.getString("endpointUrl"),
                object.getString("region"));
    }
}
