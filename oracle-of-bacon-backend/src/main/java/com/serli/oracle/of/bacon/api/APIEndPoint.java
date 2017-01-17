package com.serli.oracle.of.bacon.api;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository.Neo4jData;
import net.codestory.http.annotations.Get;
import java.util.List;
import java.util.Optional;

import org.bson.Document;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public List<Neo4jData> getConnectionsToKevinBacon(String actorName) {
        redisRepository.addSearch(actorName);
    	return neo4JRepository.getConnectionsToKevinBacon(actorName);
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) {
    	return elasticSearchRepository.getActorsSuggests(searchQuery);
    }

    @Get("last-searches")
    public List<String> last10Searches() {
    	return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public Optional<Document> getActorByName(String actorName) {
        return mongoDbRepository.getActorByName(actorName);
    }
   
}
