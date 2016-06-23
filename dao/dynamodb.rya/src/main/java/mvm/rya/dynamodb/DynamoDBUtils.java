package mvm.rya.dynamodb;
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
