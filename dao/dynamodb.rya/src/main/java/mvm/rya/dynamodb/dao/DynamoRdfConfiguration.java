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

import org.apache.hadoop.conf.Configuration;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;

public class DynamoRdfConfiguration extends RdfCloudTripleStoreConfiguration{
	
	public static final String USE_MOCK = ".useMockInstance";
	public static final String AWS_USER = "rya.aws.user";
	public static final String AWS_SECRET_KEY = "rya.aws.secret.key";
	public static final String AWS_ENDPOINT = "rya.aws.endpoint";
	public static final String DYNAMO_TABLE_NAME = "rya.dynamo.tablename";

	public DynamoRdfConfiguration(Configuration dynamoRdfConfiguration) {
		super(dynamoRdfConfiguration);
	}
	
	public boolean useMockInstance() {
		return getBoolean(USE_MOCK, false);
	}
	
	
	public DynamoRdfConfiguration(){
		super();
	}
	
	public String getAWSUserName(){
		return super.get(AWS_USER, "FakeUser");
	}
	
	public void setAWSUserName(String username){
		super.set(AWS_USER, username);
	}

	public void setAWSSecretKey(String key){
		super.set(AWS_SECRET_KEY, key);
	}

	public void setAWSEndPoint(String endpoint){
		super.set(AWS_ENDPOINT, endpoint);
	}

	public void setDynamoTablename(String name){
		super.set(DYNAMO_TABLE_NAME, name);
	}
	public String getTableName(){
		return get(DYNAMO_TABLE_NAME, "rya");
	}

	
	public String getAWSSecretKey(){
		return get(AWS_SECRET_KEY, "Fake");
	}

	public String getAWSEndPoint(){
		return get(AWS_ENDPOINT, "http://localhost:8000");
	}

	@Override
	public RdfCloudTripleStoreConfiguration clone() {
		return new DynamoRdfConfiguration(this);
	}

}
