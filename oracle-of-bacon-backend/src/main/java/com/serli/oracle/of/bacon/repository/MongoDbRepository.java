package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.Optional;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public Optional<Document> getActorByName(String name) {
    	MongoDatabase db = mongoClient.getDatabase("workshop");
    	MongoCollection<Document> actors = db.getCollection("actors");
    	Document result = actors.find(Filters.eq("name",name)).first();
        return Optional.ofNullable(result);
    }
}
