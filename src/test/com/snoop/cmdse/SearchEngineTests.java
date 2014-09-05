package com.snoop.cmdse;

import java.util.UUID;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.snoop.se.SearchEngine;

public class SearchEngineTests extends TestCase {

	@Rule public ExceptionRule rule = new ExceptionRule();
	
	public class ExceptionRule implements MethodRule {
	    @Override
	    public Statement apply(final Statement base, final FrameworkMethod method, Object target) {
	        return new Statement() {
	            @Override
	            public void evaluate() throws Throwable {
	                try {
	                    base.evaluate();
	                    Assert.fail();
	                } catch (Exception e) {
	                    //Analyze the exception here
	                }
	            }
	        };    
	    }
	}
	
	@Test
	public void testExistIndex(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "amovies";
	    
	    assertTrue(myEngine.existsIndex(indexName));
		
    }

	@Test
	public void testNotExistIndex(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "saraza";
	    
	    assertFalse(myEngine.existsIndex(indexName));
		
    }
	
	@Test
	public void testExistType(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "amovies";
	    String typeName = "movie";
	    
	    assertTrue(myEngine.existsType(indexName, typeName));
		
    }

	@Test
	public void testNotExistType(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "cars";
	    String typeName = "saraza";
	    
	    assertFalse(myEngine.existsType(indexName, typeName));
		
    }	
	
	@Test
	public void testExistDocument(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "amovies";
	    String typeName = "movie";
	    String documentId = "1";
	    
	    assertTrue(myEngine.existsDocument(indexName, typeName, documentId));
		
    }

	@Test
	public void testNotExistDocument(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "cars";
	    String typeName = "saraza";
	    String documentId = "Devil";
	    
	    assertFalse(myEngine.existsDocument(indexName, typeName, documentId));
		
    }			
	
	@Test
	public void testAddIndexAlreadyExist(){		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String indexName = "utn";
	    	    	    
	    try{ 
	    	assertTrue(myEngine.addIndex(indexName));
	        fail("Failed to assert :No exception thrown");
	    } catch(Exception ex){
	        assertNotNull("Failed to assert", ex.getMessage()); 
	        //assertEquals("Failed to assert", "Expected Message", ex.getMessage());
	    }
		
    }

	@Test
	public void testAddIndexRandom(){		
		SearchEngine myEngine = SearchEngine.getInstance();					   
		String indexName = UUID.randomUUID().toString();	    
	    	    
		assertTrue(myEngine.addIndex(indexName));
		assertTrue(myEngine.existsIndex(indexName));
		
    }	
	
	@Test
	public void testAddDocumentToIndexAlreadyCreated() throws Exception{		
		SearchEngine myEngine = SearchEngine.getInstance();				
	    String index = "utn";
	    String type = "tacs";
	    String id = "1";	    
	    	    
	    try{ 
	    	assertTrue(myEngine.addDocument(index, type, id));
	        fail("Failed to assert :No exception thrown");
	    } catch(Exception ex){
	        assertNotNull("Failed to assert", ex.getMessage()); 	        
	    }
	    
	    assertTrue(myEngine.existsDocument(index, type, id));
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
