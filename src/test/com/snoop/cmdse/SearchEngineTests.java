package com.snoop.cmdse;

import junit.framework.TestCase;

import com.snoop.se.*;

import org.junit.Test;

public class SearchEngineTests extends TestCase {

	@Test
	public void testExistIndex() throws Exception {		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "cars";
	    
	    assertTrue(myEngine.existsIndex(indexName));
		
    }

	@Test
	public void testNotExistIndex() throws Exception {		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "saraza";
	    
	    assertFalse(myEngine.existsIndex(indexName));
		
    }
	
/*
	
	@Test
	public void testFindDriverMissingBoth() throws Exception {
		//Missing on OData and ES
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "cars";
	    String documentType = "driver";
	    String documentId = "1";
	    String fieldName = "Name";
	    String valueToSearch = "Martin";  
		String valueResponse = null;
		
	    valueResponse = myEngine.searchFieldString(indexName, documentType, documentId, fieldName);
	    
		assertNotSame(valueToSearch, valueResponse);
		
    }
	
	@Test
	public void testFindDriverBoth() throws Exception {
		//Success search on OData and ES
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "cars";
	    String documentType = "driver";
	    String documentId = "1";
	    String fieldName = "Name";
	    
	    assertTrue(myEngine.searchIDOnBoth(indexName, documentType, documentId));		
    }	
	
	@Test
	public void testFindDriverMissing() throws Exception {
		//Missing on ES and found on OData----> add index on ES		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "carsssx";
	    String documentType = "driver";
	    String documentId = "1";
	    String fieldName = "Name";
	    String valueToSearch = "Mic";  		
		
	    assertTrue(myEngine.searchAndAddES(indexName, documentType, documentId, fieldName, valueToSearch));		
    }
*/
}
