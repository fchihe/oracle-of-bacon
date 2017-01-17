package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

public class CompletionLoader {
	private static AtomicInteger count = new AtomicInteger(0);
	public static final String ACTOR_TYPE = "actor";
	public static final String PEOPLE_INDEX = "people";
	public static final String SUGGESTION = "suggestions";
	// defines the size of actors to flush to avoid memory issues
	private static int maxActorsToFlush = 100000;
	private static int actorsToFlush = 0;
	private static Builder bulkBuilder;
	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Expecting 1 arguments, actual : " + args.length);
			System.err.println("Usage : completion-loader <actors file path>");
			System.exit(-1);
		}

		String inputFilePath = args[0];
		JestClient client = ElasticSearchRepository.createClient();
		createPeopleIndex(client);
		createActorMapping(client);
		// init bulk builder
		resetBulkBuilder();
	
		try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
			bufferedReader.lines().skip(1) // skip headers of CSV
			.forEach(line -> {
				// check if we reached the max number of actors to flush
				checkMustFlushActors(client);
				// clean and split (by space) the name of the actor into an array
				ArrayList<String> suggestions = prepareActorSuggestions(line);
				// get the actor in a JSON form
				String actor = createJsonActor(line, suggestions);
				bulkBuilder.addAction(new Index.Builder(actor).build());
				count.incrementAndGet();
				actorsToFlush++;
			});
		}
		// may be we have some actors left we didn't flush because it was < max actors to flush
		if(actorsToFlush > 0) {
			client.execute(bulkBuilder.build());
		}
		System.out.println("Inserted total of " + count.get() + " actors");
	}
	
	/**
	 * Create the actor with its name and the array of suggestions
	 * @param line
	 * @param suggestions
	 * @return
	 */
	private static String createJsonActor(String line, ArrayList<String> suggestions) {
		String actor = "{\"name\": "+line+", \"suggestions\" : { \"input\" : [";
		if(suggestions.size() > 0) {
			for(String s: suggestions){
				actor += "\""+ s +"\",";
			}
			if(suggestions.size() > 1) {
				actor += "\""+String.join(" ", suggestions)+"\",";
				Collections.reverse(suggestions);
				actor += "\""+String.join(" ", suggestions)+"\"";
			} else {
				actor = actor.substring(0, actor.length()-1); // delete comma
			}
		}
		actor += "]}}";
		return actor;
	}

	/**
	 * Clean the csv line and store each value separated by a space in an array
	 * @param line
	 * @return
	 */
	private static ArrayList<String> prepareActorSuggestions(String line) {
		ArrayList<String> suggestions = new ArrayList<String>(
			Arrays.asList(
				line
				.replace("'", "")
				.replace("\"","")
				.replace(",",  "")
				.trim() // cleaned csv line
				.split("\\s+") // line to array
			)
			.stream()
			.map(s -> s.trim()) // clean individual value of array
			.collect(Collectors.toList()) // collect to make a list
		);
		return suggestions;
	}
	
	/**
	 * Flush actors if we reached the limit of max actors to flush
	 * @param client
	 */
	private static void checkMustFlushActors(JestClient client) {
		if(actorsToFlush > maxActorsToFlush) {
			actorsToFlush = 0;
			try {
				client.execute(bulkBuilder.build());
			} catch (IOException e) {
				System.out.println("Error while flushing actors. Error is : ");
				e.printStackTrace();
			}
			// reset the bulk builder to avoid pushing already pushed actors
			resetBulkBuilder();
		}
	}
	
	private static void resetBulkBuilder() {
		bulkBuilder = new Bulk.Builder().defaultIndex(PEOPLE_INDEX).defaultType(ACTOR_TYPE);
	}
	
	/**
	 * Create a mapping to use Elasticsearch suggestions
	 * @param client
	 * @throws IOException
	 */
	private static void createActorMapping(JestClient client) throws IOException {
		PutMapping putMapping = new PutMapping.Builder(
				PEOPLE_INDEX,
		        ACTOR_TYPE,
		        "{"+
		        	"\""+ACTOR_TYPE+"\" : { "+ 
			        	"\"properties\" : { "+
			        		"\"name\" : {"+
			        			"\"type\" : \"string\""+
					  		"},"+
			        		"\"" + SUGGESTION + "\" : {"+
					  			"\"type\" : \"completion\""+
			        		"}"+
					   	"}"+
					"}"+
				"}"
		).build();
		client.execute(putMapping);
	}
	
	private static void createPeopleIndex(JestClient client) throws IOException {
		client.execute(new CreateIndex.Builder(PEOPLE_INDEX).build());
	}
}
