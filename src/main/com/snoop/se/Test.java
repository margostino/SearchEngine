package com.snoop.se;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

public class Test {

	private static final String ID_NOT_FOUND = "<ID NOT FOUND>";
	private static Client client;
	private static Node node;
	
	public static void main(String[] args) {
		
        final String indexName = "movies";
        final String documentType = "movie";
        final String documentId = "1";
        final String fieldName = "foo";
        final String value = "bar";       
					
        startupES();
        
        try{
        	//Create Index and set settings and mappings
        	createIndex(indexName, documentType, documentId, fieldName, value);    

			// Get document
			System.out.println(getValue(client, indexName, documentType, documentId, fieldName));

		}catch (Exception e){
			System.out.println(e.getMessage());
		}
						
		
		/*GetRequestBuilder getRequestBuilder = client.prepareGet("manufacturers", "cars", "1");
		getRequestBuilder.setFields(new String[]{"title"});
		GetResponse response = getRequestBuilder.execute().actionGet();
		String name = response.field("title").getValue().toString();
		System.out.println(name);*/

		getAllDocs(indexName, documentType);
		
		shutdownES();

	}
		
    protected static String getValue(final Client client, final String indexName, final String documentType,
        final String documentId, final String fieldName) {
        final GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
        getRequestBuilder.setFields(new String[] { fieldName });
        final GetResponse response2 = getRequestBuilder.execute().actionGet();
        if (response2.isExists()) {
            final String name = response2.getFields().get(fieldName).getValue().toString();
            return name;
        } else {
            return ID_NOT_FOUND;
        }
    }
    
    public static void startupES(){
    	// on startup
    	node = nodeBuilder().node();
    	
    	client = node.client();
    }
    
    public static  void shutdownES(){
    	// on shutdown
    	//node.close();
    }

    public static List<Map<String, Object>> getAllDocs(String indexName, String typeName){
        int scrollSize = 1000;
        List<Map<String,Object>> esData = new ArrayList<Map<String,Object>>();
        SearchResponse response = null;
        int i = 0;
        while( response == null || response.getHits().hits().length != 0){
            response = client.prepareSearch(indexName)
                    .setTypes(typeName)
                       .setQuery(QueryBuilders.matchAllQuery())
                       .setSize(scrollSize)
                       .setFrom(i * scrollSize)
                    .execute()
                    .actionGet();
            for(SearchHit hit : response.getHits()){
                esData.add(hit.getSource());
            }
            i++;
        }
        
        for (Map<String,Object> elMap : esData) {
        	for (Map.Entry<String, Object> entry : elMap.entrySet()) {
        	    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        	}
        }
        
        
        return esData;
    }	
    
    public static void createIndex(String indexName, String documentType, String documentId, String fieldName, String value) throws Exception{
    	createMapping(indexName, documentType);
    	addDocuments(indexName, documentType, documentId, fieldName, value);    	
    }	
    
    public static void addDocuments(String indexName, String documentType, String documentId, String fieldName, String value) throws Exception{

		// Add documents
		final IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, documentType, documentId);
		// build json object
		final XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
		contentBuilder.field(fieldName, value);

		indexRequestBuilder.setSource(contentBuilder);
		indexRequestBuilder.execute().actionGet();
    }
    
    public static void createMapping(String indexName, String documentType) throws Exception{
    	// MAPPING GOES HERE
    	final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
    			
    	final XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(documentType)
    				.startObject("_ttl").field("enabled", "true").field("default", "1s").endObject().endObject()
    				.endObject();
    		
    	System.out.println(mappingBuilder.string());
    	createIndexRequestBuilder.addMapping(documentType, mappingBuilder);
    			
    	// MAPPING DONE
    	createIndexRequestBuilder.execute().actionGet();   
    }
}
