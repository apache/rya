package mvm.rya.dynamodb.dao;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.persist.RdfDAOException;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.RyaNamespaceManager;

public class DynamoNamespaceManager implements RyaNamespaceManager<DynamoRdfConfiguration> {

	private static final String PREFIX = "PREFIX";
	private static final String NAMESPACE = "NS";
	private DynamoRdfConfiguration conf;
	private DynamoDB dynamoDB;
	private AmazonDynamoDB client;
	private String tableName;
	
	public DynamoNamespaceManager(DynamoRdfConfiguration conf, AmazonDynamoDB client){
		this.tableName = conf.getTableName() + "_ns";
		this.conf = conf;
		setDynamoDBClient(client);
		initDBTables();
	}
	
	public final void setDynamoDBClient(AmazonDynamoDB client){
		this.client = client;
		this.dynamoDB = new DynamoDB(client);
	}
	
	public AmazonDynamoDB getDynamoDBClient(){
		return this.client;
	}

	@Override
	public void setConf(DynamoRdfConfiguration conf) {
		this.conf = conf;
	}

	@Override
	public DynamoRdfConfiguration getConf() {
		return conf;
	}

	@Override
	public void addNamespace(String pfx, String namespace) throws RyaDAOException {
		Map<String, Object> values = new HashMap<String, Object>();
		values.put(PREFIX, pfx);
		values.put(NAMESPACE, namespace);
		BatchWriteItemSpec request  = new BatchWriteItemSpec();
		TableWriteItems tableWriteItem = new TableWriteItems(tableName);
		Item item = Item.fromMap(values);
		tableWriteItem.withItemsToPut(item);
		request.withTableWriteItems(tableWriteItem);
		dynamoDB.batchWriteItem(request);		
	}
	private void initDBTables() throws RdfDAOException {
		try {
			createDynamoTables(dynamoDB);
		}
		catch (ResourceInUseException ex){
			System.err.println("Tables on dynamo already exist.");
		}
		catch (Exception ex){
			throw new RdfDAOException(ex);
		}
		
	}

	 
	 private void createDynamoTables(DynamoDB dynamoDB) throws InterruptedException{
         System.out.println("Attempting to create namespace table; please wait...");
         CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
         		.withKeySchema(Arrays.asList(
         				new KeySchemaElement(PREFIX, KeyType.HASH),  //Partition key
         				new KeySchemaElement(NAMESPACE, KeyType.RANGE)))
         		.withAttributeDefinitions( Arrays.asList(
                        new AttributeDefinition(PREFIX, ScalarAttributeType.S),
                     new AttributeDefinition(NAMESPACE, ScalarAttributeType.S)
                     )).withProvisionedThroughput(new ProvisionedThroughput()
     				        .withReadCapacityUnits((long) 10)
     				        .withWriteCapacityUnits((long) 1));
         Table table = dynamoDB.createTable(request);

         table.waitForActive();
         System.out.println("Done creating table..");
	 }

	@Override
	public String getNamespace(String pfx) throws RyaDAOException {
    	QuerySpec spec = new QuerySpec().withHashKey(new KeyAttribute(PREFIX, pfx));
        Table table = dynamoDB.getTable(tableName);
        ItemCollection<QueryOutcome> results = table.query(spec);
        Iterator<Item> resultIt = results.iterator();
        if (resultIt.hasNext()){
        	Item returnVal = resultIt.next();
        	return returnVal.get(NAMESPACE).toString();
        }      
		return null;
	}

	@Override
	public void removeNamespace(String pfx) throws RyaDAOException {
        Table table = dynamoDB.getTable(tableName);
        table.deleteItem(new KeyAttribute(PREFIX, pfx));	
	}

	@Override
	public CloseableIteration<? extends Namespace, RyaDAOException> iterateNamespace() throws RyaDAOException {
		ScanRequest scan = new ScanRequest();
		scan.withTableName(tableName);
		ScanResult result = client.scan(scan);
		final Iterator<Map<String, AttributeValue>> itemIt = result.getItems().iterator();
		return new CloseableIteration<Namespace, RyaDAOException>(){

			@Override
			public boolean hasNext() throws RyaDAOException {
				return itemIt.hasNext();
			}

			@Override
			public Namespace next() throws RyaDAOException {
				String nameSpace = null;
				String prefix = null;
				while (nameSpace == null){
					if (itemIt.hasNext()){
						Map<String, AttributeValue> item = itemIt.next();	
						nameSpace = item.get(NAMESPACE).toString();	
						prefix = item.get(PREFIX).toString();
					}
					else {
						break;
					}
				}
				
				
				return new NamespaceImpl(prefix, nameSpace);
			}

			@Override
			public void remove() throws RyaDAOException {
				throw new RyaDAOException("not supported");
			}

			@Override
			public void close() throws RyaDAOException {
				// TODO Auto-generated method stub
				
			}
		};
	}
}
