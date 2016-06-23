package mvm.rya.dynamodb.dao;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RdfDAOException;

public class DynamoStorageStrategy {
	
	private AmazonDynamoDBClient client;
	private DynamoRdfConfiguration conf;
	private DynamoDB dynamoDB;
    public static final String NULL = "\u0000";
	private static final String tableName = "rya";
	
	public DynamoStorageStrategy(DynamoRdfConfiguration conf) throws RdfDAOException{
		this.conf = conf;
		init();
	}
	
	private void init() {
		AWSCredentials credentials = new BasicAWSCredentials(conf.getAWSUserName(), conf.getAWSSecretKey());
		client = client = new AmazonDynamoDBClient(credentials);
        client.setEndpoint(conf.getAWSEndPoint());
        dynamoDB = new DynamoDB(client);
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

	 
	 private static void createDynamoTables(DynamoDB dynamoDB) throws InterruptedException{
         System.out.println("Attempting to create table; please wait...");
         CreateTableRequest request = new CreateTableRequest().withTableName(tableName)
         		.withKeySchema(Arrays.asList(
         				new KeySchemaElement("subject", KeyType.HASH),  //Partition key
         				new KeySchemaElement("POC", KeyType.RANGE)))
         		.withAttributeDefinitions( Arrays.asList(
                     new AttributeDefinition("subject", ScalarAttributeType.S),
                     new AttributeDefinition("POC", ScalarAttributeType.S),
                     new AttributeDefinition("predicate", ScalarAttributeType.S),
                     new AttributeDefinition("object", ScalarAttributeType.S)
//                     new AttributeDefinition("context", ScalarAttributeType.S)
                     )).withProvisionedThroughput(new ProvisionedThroughput()
     				        .withReadCapacityUnits((long) 10)
     				        .withWriteCapacityUnits((long) 1))
         		.withGlobalSecondaryIndexes(getSecondaryIndexes());
         Table table = dynamoDB.createTable(request);

         table.waitForActive();
         System.out.println("Attempting to create table; please wait...");
	 }

	private static List<GlobalSecondaryIndex> getSecondaryIndexes() {
		GlobalSecondaryIndex predicate = new GlobalSecondaryIndex()
				.withIndexName("predicate").withKeySchema(Arrays.asList(
				new KeySchemaElement("predicate", KeyType.HASH)))
				.withProvisionedThroughput(new ProvisionedThroughput()
				        .withReadCapacityUnits((long) 10)
				        .withWriteCapacityUnits((long) 1))
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		GlobalSecondaryIndex object = new GlobalSecondaryIndex().
				withIndexName("object")
				.withKeySchema(Arrays.asList(
				new KeySchemaElement("object", KeyType.HASH)))
				.withProvisionedThroughput(new ProvisionedThroughput()
				        .withReadCapacityUnits((long) 10)
				        .withWriteCapacityUnits((long) 1))
				.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
		return Arrays.asList(predicate, object);
	}

	public void close() {
		// not sure what to do here
	}

	public void add(RyaStatement statement) {
		Map<String, AttributeValue> values = getItem(statement);
		PutItemRequest request = new PutItemRequest()
				.withTableName(tableName).withItem(values);
		client.putItem(request);
	}

	private Map<String, AttributeValue> getItem(RyaStatement statement) {
		String contextValue = "";
		if (statement.getContext() != null){
			contextValue = statement.getContext().getData();
		}
		Map<String, AttributeValue> values = new HashMap<String, AttributeValue>();
		values.put("subject", new AttributeValue( statement.getSubject().getData()));
		values.put("predicate", new AttributeValue( statement.getPredicate().getData()));
		values.put("POC", new AttributeValue( statement.getPredicate().getData() + NULL + statement.getObject().getData()
				+ NULL + statement.getObject().getDataType().toString() + NULL + contextValue));
		values.put("context", new AttributeValue(contextValue));
		values.put("object", new AttributeValue( statement.getObject().getData()
				+ NULL + statement.getObject().getDataType().toString()));
		return values;
	}

	private Map<String, Object> getItemMap(RyaStatement statement) {
		String contextValue = "";
		if (statement.getContext() != null){
			contextValue = statement.getContext().getData();
		}
		Map<String, Object> values = new HashMap<String, Object>();
		values.put("subject", statement.getSubject().getData());
		values.put("predicate",statement.getPredicate().getData());
		values.put("POC", statement.getPredicate().getData() + NULL + statement.getObject().getData()
				+ NULL + statement.getObject().getDataType().stringValue() + NULL + contextValue);
		values.put("context", contextValue);
		values.put("object", statement.getObject().getData()
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

	
	public ItemCollection<QueryOutcome> getQuery(RyaStatement stmt){
        final RyaURI subject = stmt.getSubject();
        final RyaURI predicate = stmt.getPredicate();
        final RyaType object = stmt.getObject();
        final RyaURI context = stmt.getContext();
        Table table = dynamoDB.getTable(tableName);
        
        if (subject != null){
        	QuerySpec spec = new QuerySpec().withHashKey(new KeyAttribute("subject", subject.getData()));
            ValueMap map = new ValueMap();
            if (object != null){
            	map.put("object", object.getData() + NULL + object.getDataType().stringValue());
            }
            if (predicate != null){
            	map.put("predicate", predicate.getData());
            }
            if (context != null){
            	map.put("context", context.getData());
            }
            return table.query(spec);
        }
        else if (predicate != null) {
    		QuerySpec spec = new QuerySpec().withHashKey(new KeyAttribute("predicate", predicate.getData()));
            ValueMap map = new ValueMap();
            if (object != null){
            	map.put("object", object.getData() + NULL + object.getDataType().stringValue());
            }
            if (context != null){
            	map.put("context", context.getData());
            }
            Index index = table.getIndex("predicate");
            return index.query(spec);
        }
        else if (object != null) {
    		QuerySpec spec = new QuerySpec().withHashKey(new KeyAttribute("object", object.getData() + NULL + object.getDataType().stringValue()));
            ValueMap map = new ValueMap();
            if (context != null){
            	map.put("context", context.getData());
            }
            Index index = table.getIndex("object");
            return index.query(spec);
        }
 		return null;

	}

}
