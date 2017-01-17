package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Path;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"));
    }

    public List<Neo4jData> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        Record record = session.run(
        "MATCH path = shortestPath "
        +"("
        +"(kevin {name:'Bacon, Kevin (I)'})"
        +"        -[PLAYED_IN*]-"
        +"(actor {name:'"+ actorName +"'})"
        +")"
        +"return path;"
        ).single();
        Path p = record.get("path").asPath();
        List<Neo4jData> result = new ArrayList<Neo4jData>();
        
        //For each neo4j node, create a GraphNode
        p.nodes().forEach(node -> {
	        long id = node.id();
	        String name = node.containsKey("name") ? node.get("name").asString() : node.get("title").asString();
	        String type = Iterables.get(node.labels(),0);
	        result.add(new Neo4jData( new GraphNode(id, name , type)));
    	});
        
        //For each neo4j relationship, create a graph edge
        p.relationships().forEach(relationship -> {
        	long id = relationship.id();
        	long source = relationship.startNodeId();
        	long target = relationship.endNodeId();
        	String value = relationship.type();
        	result.add(new Neo4jData(new GraphEdge(id, source, target, value)));
        });
        
        return result;
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
    public static class Neo4jData {
        public final GraphItem data;

        public Neo4jData(GraphItem data) {
			this.data = data;
		}
    }
}
