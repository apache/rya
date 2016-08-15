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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.xspec.QueryExpressionSpec;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RdfDAOException;

public class DynamoStorageStrategy {
	
	private static final String POC = "POC";
	private static final String CONTEXT = "context";
	private static final String OBJECT = "obj";
	private static final String PREDICATE = "predicate";
	private static final String SUBJECT = "subject";
	private DynamoRdfConfiguration conf;
	private DynamoDB dynamoDB;
    public static final String NULL = "\u0000";
	private String tableName = "rya";
	
	public DynamoStorageStrategy(DynamoRdfConfiguration conf, DynamoDB dynamoDB) throws RdfDAOException{
		this.conf = conf;
		this.dynamoDB = dynamoDB;
		init();
	}
	
	private void init() {
        this.tableName = conf.getTableName();
        initDBTables();
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
         System.out.println("Attempting to create table; please wait...");
         CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
         		.withKeySchema(Arrays.asList(
         				new KeySchemaElement(SUBJECT, KeyType.HASH),  //Partition key
         				new KeySchemaElement(POC, KeyType.RANGE)))
         		.withAttributeDefinitions( Arrays.asList(
                     new AttributeDefinition(SUBJECT, ScalarAttributeType.S),
                     new AttributeDefinition(POC, ScalarAttributeType.S),
                     new AttributeDefinition(PREDICATE, ScalarAttributeType.S),
                     new AttributeDefinition(OBJECT, ScalarAttributeType.S)
//                     new AttributeDefinition("context", ScalarAttributeType.S)
                     )).withProvisionedThroughput(new ProvisionedThroughput()
     				        .withReadCapacityUnits((long) 10)
     				        .withWriteCapacityUnits((long) 1))
         		.withGlobalSecondaryIndexes(getSecondaryIndexes());
         Table table = dynamoDB.createTable(request);

         table.waitForActive();
         System.out.println("Done creating table..");
	 }

	private static List<GlobalSecondaryIndex> getSecondaryIndexes() {
		GlobalSecondaryIndex predicate = new GlobalSecondaryIndex()
				.withIndexName(PREDICATE).withKeySchema(Arrays.asList(
				new KeySchemaElement(PREDICATE, KeyType.HASH)))
				.withProvisionedThroughput(new ProvisionedThroughput()
				        .withReadCapacityUnits((long) 10)
				        .withWriteCapacityUnits((long) 1))
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		GlobalSecondaryIndex object = new GlobalSecondaryIndex().
				withIndexName(OBJECT)
				.withKeySchema(Arrays.asList(
				new KeySchemaElement(OBJECT, KeyType.HASH)))
				.withProvisionedThroughput(new ProvisionedThroughput()
				        .withReadCapacityUnits((long) 10)
				        .withWriteCapacityUnits((long) 1))
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		return Arrays.asList(predicate, object);
	}

	public void close() {
		
	}

	public void add(RyaStatement statement) {
		Map<String, Object> values = getItemMap(statement);
		BatchWriteItemSpec request  = new BatchWriteItemSpec();
		TableWriteItems tableWriteItem = new TableWriteItems(tableName);
		Item item = Item.fromMap(values);
		tableWriteItem.withItemsToPut(item);
		request.withTableWriteItems(tableWriteItem);
		dynamoDB.batchWriteItem(request);
	}

	private Map<String, Object> getItemMap(RyaStatement statement) {
		String contextValue = NULL;
		if (statement.getContext() != null){
			contextValue = statement.getContext().getData();
		}
		Map<String, Object> values = new HashMap<String, Object>();
		values.put(SUBJECT, statement.getSubject().getData());
		values.put(PREDICATE,statement.getPredicate().getData());
		values.put(POC, statement.getPredicate().getData() + NULL + statement.getObject().getData()
				+ NULL + statement.getObject().getDataType().stringValue() + NULL + contextValue);
		values.put(CONTEXT, contextValue);
		values.put(OBJECT, statement.getObject().getData()
				+ NULL + statement.getObject().getDataType().toString());
		return values;
	}

	public void add(Iterator<RyaStatement> statements) {
		TableWriteItems write = new TableWriteItems(tableName);
		while(statements.hasNext()){
			write.addItemToPut(Item.fromMap(getItemMap(statements.next())));
		}
		dynamoDB.batchWriteItem(write);
		
	}

	public Collection<ItemCollection<QueryOutcome>> getBatchQuery(Iterable<RyaStatement> query){
		//  TODO could do this more efficiently with a batch get?
		// also ignoring most of the constraints of the query (max size, timeout, etc.)
		Collection<ItemCollection<QueryOutcome>> returnVal = 
				new ArrayList<ItemCollection<QueryOutcome>>();
		Iterator<RyaStatement> queries = query.iterator();
		while (queries.hasNext()){
			returnVal.add(getQuery(queries.next()));
		}
		return returnVal;
	}
	
	public ItemCollection<QueryOutcome> getQuery(RyaStatement stmt){
        final RyaURI subject = stmt.getSubject();
        final RyaURI predicate = stmt.getPredicate();
        final RyaType object = stmt.getObject();
        final RyaURI context = stmt.getContext();
        Table table = dynamoDB.getTable(tableName);
        
        if (subject != null){
        	QuerySpec spec = new QuerySpec()
        	.withKeyConditionExpression(SUBJECT + " = :v_subj");
            ValueMap map = new ValueMap();
        	map = map.withString(":v_subj", subject.getData());
            if (object != null){
            	spec = spec.withFilterExpression(OBJECT + " = :v_obj");
            	map.put(":v_obj", object.getData() + NULL + object.getDataType().stringValue());
            }
            if (predicate != null){
            	spec = spec.withFilterExpression(PREDICATE + " = :v_pred");
            	map.put(":v_pred", predicate.getData());
            }
            if (context != null){
               	spec = spec.withFilterExpression(CONTEXT + " = :v_context");
               	map.put(CONTEXT, context.getData());
            }
            spec  = spec.withValueMap(map);
            return table.query(spec);
        }
        else if (predicate != null) {
    		QuerySpec spec = new QuerySpec().withKeyConditionExpression(PREDICATE + " = :v_pred");
            ValueMap map = new ValueMap();
        	map.put(":v_pred", predicate.getData());
           if (object != null){
              	spec = spec.withFilterExpression(OBJECT + " = :v_obj");
            	map.put(":v_obj", object.getData() + NULL + object.getDataType().stringValue());
            }
            if (context != null){
               	spec = spec.withFilterExpression(CONTEXT + " = :v_context");
               	map.put(CONTEXT, context.getData());
            }
            spec  = spec.withValueMap(map);
           Index index = table.getIndex(PREDICATE);
            return index.query(spec);
        }
        else if (object != null) {
    		QuerySpec spec = new QuerySpec().withKeyConditionExpression(OBJECT + " = :v_obj");
            ValueMap map = new ValueMap();
            map.put(":v_obj", object.getData() + NULL + object.getDataType().stringValue());
            if (context != null){
              	spec = spec.withFilterExpression(CONTEXT + " = :v_context");
               	map.put(CONTEXT, context.getData());
            }
            spec  = spec.withValueMap(map);
           Index index = table.getIndex(OBJECT);
            return index.query(spec);
        }
 		return null;

	}

	public RyaStatement convertToStatement(Item item) {
		String subject = item.getString(SUBJECT);
		String predicate = item.getString(PREDICATE);
		String compositeObject = item.getString(OBJECT);
		String context = item.getString(CONTEXT);
		String[] objectDelim = compositeObject.split(NULL);
		ValueFactory vf = new ValueFactoryImpl();
		RyaType object = new RyaType(vf.createURI(objectDelim[1]), objectDelim[0]);
		RyaStatement stmt = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), object);
		if (!(context == null) && !context.equalsIgnoreCase(NULL)){
			stmt.setContext(new RyaURI(context));
		}
		return stmt;
	}

	public void delete(RyaStatement stmt) {
		Table table = dynamoDB.getTable(tableName);
		Iterator<Item> queryResult = getQuery(stmt).iterator();
		while(queryResult.hasNext()) {
			Item item = queryResult.next();
			DeleteItemSpec delete = new DeleteItemSpec().withPrimaryKey(new KeyAttribute(SUBJECT, item.get(SUBJECT)), 
					new KeyAttribute(POC, item.get(POC)));
			table.deleteItem(delete);
		}
	}

	public void delete(Iterator<RyaStatement> statements) {
		while(statements.hasNext()){
			delete(statements.next());
		}
	}

	public void dropTables() {
		dynamoDB.getTable(tableName).delete(); // DANGEROUS!
	}

}
