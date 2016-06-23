package mvm.rya.dynamodb.dao;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;

public class DynamoRdfConfiguration extends RdfCloudTripleStoreConfiguration{
	
	public static final String AWS_USER = "rya.aws.user";
	public static final String AWS_SECRET_KEY = "rya.aws.secret.key";
	public static final String AWS_ENDPOINT = "rya.aws.endpoint";

	public DynamoRdfConfiguration(DynamoRdfConfiguration dynamoRdfConfiguration) {
		super(dynamoRdfConfiguration);
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

	public String getAWSSecretKey(){
		return super.get(AWS_SECRET_KEY, "Fake");
	}

	public String getAWSEndPoint(){
		return super.get(AWS_ENDPOINT, "http://localhost:8000");
	}

	@Override
	public RdfCloudTripleStoreConfiguration clone() {
		return new DynamoRdfConfiguration(this);
	}

}
