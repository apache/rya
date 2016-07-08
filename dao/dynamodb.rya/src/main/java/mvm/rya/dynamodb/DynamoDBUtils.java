package mvm.rya.dynamodb;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;

import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.dynamodb.dao.DynamoRdfConfiguration;

public class DynamoDBUtils {

	
	public static AmazonDynamoDB getDynamoDBClientFromConf(DynamoRdfConfiguration conf) throws RyaDAOException{
		if (conf.useMockInstance()){
			try {
				return DynamoDBEmbedded.create(File.createTempFile("temp", "dat"));
			} catch (IOException e) {
				throw new RyaDAOException(e);
			}
		}
		else {
			AWSCredentials credentials = new BasicAWSCredentials(conf.getAWSUserName(), conf.getAWSSecretKey());
			AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
	        client.setEndpoint(conf.getAWSEndPoint());
	        return client;
		}
	}
}
