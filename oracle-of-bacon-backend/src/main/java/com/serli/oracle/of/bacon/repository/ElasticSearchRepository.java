package com.serli.oracle.of.bacon.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.internal.LinkedTreeMap;
import com.serli.oracle.of.bacon.loader.elasticsearch.CompletionLoader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import io.searchbox.core.SuggestResult.Suggestion;

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
		String query = "{" +
					   "	\""+CompletionLoader.ACTOR_TYPE+"\": {" +
					   "    	\"text\" : \""+searchQuery+"\","+
					   "    	\"completion\" : {"+
					   "    		\"field\" : \""+CompletionLoader.SUGGESTION+"\""+
					   "        }" +
					   "    }" +
					   "}";
		Suggest search = new Suggest.Builder(query)
				.addIndex(CompletionLoader.PEOPLE_INDEX)
				.build();
		try {
			SuggestResult result = jestClient.execute(search);
			List<Suggestion> suggestions = result.getSuggestions(CompletionLoader.ACTOR_TYPE);
			List<String> actorNames = getSuggestedActorNames(suggestions);
			return actorNames;
		} catch (IOException e) {
			System.out.println("Error while suggesting actor names. Error is : ");
			e.printStackTrace();
		}
		return null;
	}

	private List<String> getSuggestedActorNames(List<Suggestion> suggestions) {
		Iterator<Suggestion> suggestionIterator = suggestions.iterator();
		List<String> actorNames = new ArrayList<String>();
		// retreive the names of the actors from the suggestions
		while (suggestionIterator.hasNext()) {
			Suggestion entry = suggestionIterator.next(); // suggestion
			for (Map<String, Object> option : entry.options) { // options of suggestion
				LinkedTreeMap source = (LinkedTreeMap) option.get("_source");
				actorNames.add((String) source.get("name"));  // name of the actor
			}
		}
		return actorNames;
	}


}
