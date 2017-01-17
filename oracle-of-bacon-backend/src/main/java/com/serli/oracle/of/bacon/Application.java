package com.serli.oracle.of.bacon;

import com.serli.oracle.of.bacon.api.APIEndPoint;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;

import net.codestory.http.WebServer;

import static com.serli.oracle.of.bacon.utils.EnvUtils.getenv;

import java.util.List;

public class Application {

    public static void main(String[] args) {
        WebServer webServer = new WebServer();
        webServer.configure(routes -> {
            routes.add(new APIEndPoint());
        });

        String port = getenv("PORT", "8000");
    }
}
