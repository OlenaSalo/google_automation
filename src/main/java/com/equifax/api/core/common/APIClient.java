package com.equifax.api.core.common;

import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static io.restassured.RestAssured.given;

@Component
public class APIClient {
    Map<String, String> headersAuth = new HashMap<>();

    Map<String, String> headersMap = new HashMap<>();

    private final static String USER_GET_ENDPOINT = "getAll";

    private final static String USER_UPDATE_ENDPOINT = "posts";
    private final static String USER_POST_ENDPOINT = "posts";
    private final static String USER_DESTROY_ENDPOINT = "?/{id}";

    private String token;

    public APIClient() {
        this.headersAuth.put("client_id", "ews_dm_inquiry_csfclient");
        this.headersAuth.put("client_secret", "27BEE1775EADE8E4D748DFCF43EF622397392A48D792E34E28A3F8C696");
        this.headersAuth.put("grant_type", "client_credentials");
        RestAssured.useRelaxedHTTPSValidation();
        this.token = auth2Granter();
    }

    /**
     * @param endPoint get full url
     * @return full url
     */
    private String getFullUrl(String endPoint) {
        this.headersMap.put("Content-Type", "application/json");
        return ConfigReader.getProperty("INQUIRY_API_URI") + endPoint;
    }

    private String auth2Granter() {
        RestAssured.baseURI = ConfigReader.getProperty("INQUIRY_AUTH_API");
        Response request = RestAssured.given()
                .config(RestAssured.config()
                        .encoderConfig(EncoderConfig.encoderConfig()
                                .encodeContentTypeAs("x-www-form-urlencoded",
                                        ContentType.URLENC)))
                .contentType("application/x-www-form-urlencoded; charset=UTF-8")
                .formParams(headersAuth)
                .post("/token.oauth2");
        String repossessing = request.asString();
        io.restassured.path.json.JsonPath js = new io.restassured.path.json.JsonPath(repossessing);
        return js.get("access_token");
    }

    /**
     * Retrieves the user.
     *
     * @return ValidatableResponse for the endpoint
     */
    public ValidatableResponse getUser(long SSN) {

        return given()
                .header("Authorization", "Bearer " + token)
                .queryParam("ssn", SSN)
                .when()
                .get(this.getFullUrl(USER_GET_ENDPOINT))
                .then();
    }

    /**
     * Creates a user
     *
     * @return ValidatableResponse for the endpoint
     */
    public ValidatableResponse createUser(String user) {
        return given()
                .header("Authorization", "Bearer " + token)
                .body(headersMap)
                .when()
                .post(this.getFullUrl(USER_POST_ENDPOINT))
                .then();
    }

    /**
     * Update a user
     *
     * @return ValidatableResponse for the endpoint
     */
    public ValidatableResponse updateUser(String user) {
        return given()
                .header("Authorization", "Bearer " + token)
                .body(user)
                .when()
                .post(this.getFullUrl(USER_UPDATE_ENDPOINT))
                .then();
    }

    /**
     * Deletes user based on id
     *
     * @return ValidatableResponse for the endpoint
     */
    public ValidatableResponse deleteUserWithId(long userId) {
        return given()
                .header("Authorization", "Bearer " + token)
                .queryParam("id", userId)
                .when()
                .delete(this.getFullUrl(USER_DESTROY_ENDPOINT))
                .then();
    }

    /**
     * @return Random long for id
     */
    public int generateRandomId() {
        return 100000 + (int) (Math.random() * ((100000000 - 100000) + 1));
    }


}
