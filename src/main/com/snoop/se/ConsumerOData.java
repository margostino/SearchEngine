package com.snoop.se;

import org.odata4j.consumer.ODataConsumer;
import org.odata4j.consumer.ODataConsumers;
import org.odata4j.core.OEntity;

public class ConsumerOData {
	private static ConsumerOData instance = null;
	private static ODataConsumer consumer = null;
	
	protected ConsumerOData() {
		      // Exists only to defeat instantiation.
	}
	
	public static ConsumerOData getInstance() {
		if(instance == null) {	
			instance = new ConsumerOData();
		}
		return instance;
	}
	
	public ConsumerOData createInstance(){
		// create consumer instance
		String serviceUrl = "http://services.odata.org/OData/OData.svc/";
		consumer = ODataConsumers.create(serviceUrl);
		
		return this;
	}
	
	public void showEntityAndProp(String entity, String property){
		// list category names
		for (OEntity category : consumer.getEntities(entity).execute()) {
			String categoryName = category.getProperty(property, String.class).getValue();
			System.out.println("Result: " + categoryName);
		}
	}
}
