package com.serli.oracle.of.bacon.repository;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.gson.JsonObject;

public class ElasticSearchRepository {

	private final JestClient jestClient;

	public ElasticSearchRepository() {
		jestClient = createClient();

	}

	public static JestClient createClient() {
		JestClient jestClient;
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.readTimeout(60000)
				.build());

		jestClient = factory.getObject();
		return jestClient;
	}

	public List<String> getActorsSuggests(String searchQuery) {
		String query = "{\n" +
					   "	\"query\": {\n" +
					   "    	\"match\" : {\n"+
					   "       		\"name\" : {\n"+
					   "       			\"query\" : \""+searchQuery+"\",\n"+
					   "       			\"fuzziness\" : \"1\",\n"+
					   "       			\"operator\" : \"and\"\n "+
					   "        	}\n" +
					   "        }\n" +
					   "    }\n" +
					   "}";
		Search search = new Search.Builder(query)
				.addIndex("people")
				.addType("actor")
				.build();
		try {
			SearchResult result = jestClient.execute(search);
			List<String> actorNames = result.getHits(JsonObject.class)
										.stream()
										.map(hit -> hit.source.get("name").getAsString())
										.collect(Collectors.toList());
			return actorNames;
		} catch (IOException e) {
			System.out.println("Error while suggesting actor names. Error is : ");
			e.printStackTrace();
		}
		return null;
	}


}
