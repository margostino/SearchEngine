package com.snoop.se;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataDeltaFeed;
import org.apache.olingo.odata2.core.ep.entry.ODataEntryImpl;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import com.snoop.olingo.OlingoSampleApp;

public class SearchEngine {
	
	private static SearchEngine instance = null;
	private static Client client;
	private static Node node;
	private static final String ID_NOT_FOUND = "<ID NOT FOUND>";
	private static OlingoSampleApp appOdata;
	private static Edm edm;
	
	public static final String HTTP_METHOD_PUT = "PUT";
	public static final String HTTP_METHOD_POST = "POST";
	public static final String HTTP_METHOD_GET = "GET";
	private static final String HTTP_METHOD_DELETE = "DELETE";

	public static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
	public static final String HTTP_HEADER_ACCEPT = "Accept";

	public static final String APPLICATION_JSON = "application/json";
	public static final String APPLICATION_XML = "application/xml";
	public static final String APPLICATION_ATOM_XML = "application/atom+xml";
	public static final String APPLICATION_FORM = "application/x-www-form-urlencoded";
	public static final String METADATA = "$metadata";
	public static final String INDEX = "/index.jsp";
	public static final String SEPARATOR = "/";    
	
	public static final boolean PRINT_RAW_CONTENT = true;
	
	public static final String SERVICE_OD_URL = "http://localhost:8080/cars-annotations-sample/MyFormula.svc";
	public static final String USED_FORMAT = APPLICATION_JSON;
	
	
	protected SearchEngine() {	      
		startupES();
		appOdata = new OlingoSampleApp();
		try{
			edm = appOdata.readEdm(SERVICE_OD_URL);
		}catch (Exception e){
			print(e.getMessage());
		}
		
	}
	
	public static SearchEngine getInstance() {
		if(instance == null) {	
			instance = new SearchEngine();
		}
		return instance;
	}	

    public void startupES(){
    	// on startup    	
    	//node = nodeBuilder().node();    
    	//client = node.client();    	  
    	client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300)); 
    }
    
    public void shutdownES(){
    	// on shutdown
    	node.close();
    }

	public static Client getClient() {
		return client;
	}

	public static Node getNode() {
		return node;
	}    
	
	public String searchFieldString(String indexName, String documentType, String documentId, String fieldName){
		String name = "";
		try{
			GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
			getRequestBuilder.setFields(new String[]{fieldName});
			GetResponse response = getRequestBuilder.execute().actionGet();
			name = response.getField(fieldName).getValue().toString();
		}catch (Exception e){
			print(e.getMessage());
		}
		
		return name;
	}
	
	public boolean existsIndex(String index){
		return client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();		
	}
	
	public boolean existsType(String index, String type){
		return client.admin().indices().typesExists(new TypesExistsRequest(new String[]{ index }, type)).actionGet().isExists();		
	}

	public boolean existsDocument(String index, String type, String id){
		boolean result = false;
		if (existsIndex(index) && existsType(index, type)){
			GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);			
			GetResponse response = getRequestBuilder.execute().actionGet();
			result = response.isExists();
		}
		return result;		
	}
	
	/*public boolean addDocument(String index, String type, String id){
		boolean result = false;
		if (existsIndex(index) && existsType(index, type)){
			GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);			
			GetResponse response = getRequestBuilder.execute().actionGet();
			result = response.isExists();
		}
		return result;		
	}*/
	
	public boolean searchIDOnBoth(String indexName, String documentType, String documentId){
		boolean result = false;
		try{
			GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);			
			GetResponse response = getRequestBuilder.execute().actionGet();
			
			result = true;
		}catch (Exception e){
			print(e.getMessage());
		}
		
		return result;
	}
	
	public boolean searchAndAddES(String indexName, String documentType, String documentId, String fieldName, String value){		
		boolean result = false;
		
		try{
			GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
			getRequestBuilder.setFields(new String[]{fieldName});
			GetResponse response = getRequestBuilder.execute().actionGet();
			
			if (response.isExists())
				result = searchOnOData(indexName, documentType, documentId, fieldName, value);
			
		}catch (Exception e){
			print(e.getMessage());
			
			result = searchOnOData(indexName, documentType, documentId, fieldName, value);														
		}
		
		//Check
		GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
		getRequestBuilder.setFields(new String[]{fieldName});
		GetResponse response = getRequestBuilder.execute().actionGet();
		
		print(String.valueOf(response.isExists()));
		
		return result;
	}	
	
	private boolean searchOnOData(String indexName, String documentType, String documentId, String fieldName, String value){
		boolean result = false;
		
		try{
			//Search on OData
		    ODataEntry entryExpanded = appOdata.readEntry(edm, SERVICE_OD_URL, USED_FORMAT, convertUpperInitial(indexName), convertSingleQuotes(documentId), convertUpperInitial(documentType));
		    print("Single Entry with expanded Cars relation:\n" + prettyPrint(entryExpanded));
		    ODataEntryImpl oe = (ODataEntryImpl) entryExpanded.getProperties().get(convertUpperInitial(documentType));

		    if (oe.getProperties().get("Id").toString().equals(documentId)){
		    	//Esta en OData pero no en ES
		    	result = addOnEngine(indexName, documentType, documentId, fieldName, value);			    	
			}
		}catch(Exception e){
			print(e.getMessage());
			result = addOnEngine(indexName, documentType, documentId, fieldName, value);
			//result = false;
		}
		
		return result;
	}
    public boolean addOnEngine(String indexName, String documentType, String documentId, String fieldName, String value){
    	boolean result = false;
    	try{
        	//Create Index and set settings and mappings
        	createIndex(indexName, documentType, documentId, fieldName, value);    

    		// Get document
        	print(getValue(client, indexName, documentType, documentId, fieldName));
        	
        	result = true;
        	
    	}catch (Exception e){
    		print(e.getMessage());    		
    	}
    	return result;
    }    

	public void createIndex(String indexName, String documentType, String documentId, String fieldName, String value) throws Exception{
		createMapping(indexName, documentType);
		addDocument(indexName, documentType, documentId, fieldName, value);    	
	}	
	
	public boolean addIndex(String indexName){		
		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
		// MAPPING DONE
		return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
	}
	
	/*public boolean addTypeToIndex(String indexName, String typeName){
		if (existsIndex(indexName))			
			XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(typeName);

		IndexRequest request = new IndexRequest();
		request.p
		GetRequestBuilder getRequestBuilder = client.index(request) .prepareGet(indexName);
		// MAPPING DONE
		return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
	}*/
	
	public boolean addDocument(String indexName, String documentType, String documentId, String fieldName, String value) throws Exception{
		boolean result = false;
		if (!existsDocument(indexName, documentType, documentId)){
			// Add documents
			IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, documentType, documentId);
			// build json object
			XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
			
			if (fieldName!= null && value!=null)
				contentBuilder.field(fieldName, value);

			indexRequestBuilder.setSource(contentBuilder);
			result = indexRequestBuilder.execute().actionGet().isCreated();
		}
		
		return result;
	}
	
	public boolean addDocument(String indexName, String documentType, String documentId) throws Exception{
		return addDocument(indexName, documentType, documentId, null, null);
		
	}

	public static void createMapping(String indexName, String documentType) throws Exception{
		// MAPPING GOES HERE
		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
			
		XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(documentType)
				.startObject("_ttl").field("enabled", "true").field("default", "1s").endObject().endObject()
				.endObject();
		
		print(mappingBuilder.string());
		createIndexRequestBuilder.addMapping(documentType, mappingBuilder);
			
		// MAPPING DONE
		createIndexRequestBuilder.execute().actionGet();   
	}

    protected static String getValue(final Client client, final String indexName, final String documentType,
            String documentId, final String fieldName) {
            GetRequestBuilder getRequestBuilder = client.prepareGet(indexName, documentType, documentId);
            getRequestBuilder.setFields(new String[] { fieldName });            
            GetResponse response2 = getRequestBuilder.execute().actionGet();
            
            if (response2.isExists()) {
                String name = response2.getFields().get(fieldName).getValue().toString();
                return name;
            } else {
                return ID_NOT_FOUND;
            }
     }
    
    public List<Map<String, Object>> getAllDocs(String indexName, String typeName){
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
        		print("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        	}
        }
        
        
        return esData;
    }  

	private static String prettyPrint(ODataEntry createdEntry) {
		    return prettyPrint(createdEntry.getProperties(), 0);
	}
	
	private static String prettyPrint(Map<String, Object> properties, int level) {
		    StringBuilder b = new StringBuilder();
		    Set<Entry<String, Object>> entries = properties.entrySet();

		    for (Entry<String, Object> entry : entries) {
		      intend(b, level);
		      b.append(entry.getKey()).append(": ");
		      Object value = entry.getValue();
		      if(value instanceof Map) {
		        value = prettyPrint((Map<String, Object>)value, level+1);
		        b.append(value).append("\n");
		      } else if(value instanceof Calendar) {
		        Calendar cal = (Calendar) value;
		        value = SimpleDateFormat.getInstance().format(cal.getTime());
		        b.append(value).append("\n");
		      } else if(value instanceof ODataDeltaFeed) {
		        ODataDeltaFeed feed = (ODataDeltaFeed) value;
		        List<ODataEntry> inlineEntries =  feed.getEntries();
		        b.append("{");
		        for (ODataEntry oDataEntry : inlineEntries) {
		          value = prettyPrint((Map<String, Object>)oDataEntry.getProperties(), level+1);
		          b.append("\n[\n").append(value).append("\n],");
		        }
		        b.deleteCharAt(b.length()-1);
		        intend(b, level);
		        b.append("}\n");
		      } else {
		        b.append(value).append("\n");
		      }
		    }
		    // remove last line break
		    b.deleteCharAt(b.length()-1);
		    return b.toString();
	}

	private static void intend(StringBuilder builder, int intendLevel) {
		for (int i = 0; i < intendLevel; i++) {
			builder.append("  ");
		}
	}	
	
	private static void print(String content) {
		 System.out.println(content);
	}
	
	private String convertUpperInitial(String value){
		return value.substring(0,1).toUpperCase()+value.substring(1,value.length());
	}
	
	private String convertSingleQuotes(String value){
		return "'" + value + "'";
	}
}
