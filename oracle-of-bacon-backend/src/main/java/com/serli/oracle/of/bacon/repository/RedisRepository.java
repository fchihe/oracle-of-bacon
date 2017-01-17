package com.serli.oracle.of.bacon.repository;

import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisRepository {
	
	private Jedis jedis;
	
    public RedisRepository() {
    	jedis = new Jedis("localhost");
	}

    /**
     * Get the first 10 items in the list of actors searched
     * @return
     */
	public List<String> getLastTenSearches() {
        return jedis.lrange("actorSearches", 0, 9);
    }
	
	/**
	 * Add the actor name to the beginning the actors searched list
	 * @param actorName
	 */
	public void addSearch(String actorName){
		jedis.lpush("actorSearches", actorName);
		jedis.ltrim("actorSearches", 0, 9);
	}
}
