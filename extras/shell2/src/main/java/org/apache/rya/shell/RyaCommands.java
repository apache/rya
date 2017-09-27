/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.shell;

import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.Objects;

import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration.MongoDBIndexingConfigBuilder;
import org.apache.rya.mongodb.MockMongoFactory;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.model.Value;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * Rya Shell commands that have to do with common tasks (loading and querying data)
 */
@Component
public class RyaCommands implements CommandMarker {

    private static final Logger log = LoggerFactory.getLogger(RyaCommands.class);
    public static final String CONNECT_INSTANCE_CMD = "connect-rya";
    public static final String DISCONNECT_INSTANCE_CMD = "disconnect-rya";
    public static final String QUERY_RYA_CMD = "sparql-query-rya";
    public static final String CONNECT_MONGO_INSTANCE_CMD = "connect-mongo-rya";
    public static final String DISCONNECT_MONGO_INSTANCE_CMD = "disconnect-mongo-rya";
    public static final String QUERY_MONGO_RYA_CMD = "sparql-query-mongorya";
    
    private String theHostName;
    private boolean connectStatus;
    private String connectedOutputStatus;
    private int responseCode;
    private String full_path;
    private String query_result;
    
    private boolean connectStatusMongo;
    private MongoConnector mongoc;
    private SailRepositoryConnection conn;
    private int mongo_times_connected;
    
    public RyaCommands()
    {
    	theHostName=null;
    	connectStatus=false;
    	connectedOutputStatus=null;
    	responseCode=0;
    	full_path=null;
    	query_result=null;
    	mongoc=new MongoConnector();
    	conn=null;
    	connectStatusMongo=false;
    	mongo_times_connected=0;
    }
    
    @CliCommand(value = DISCONNECT_INSTANCE_CMD, help = "Disconnect from a specific Rya instance")
    public String diconnectToInstance() {
    	if(connectStatus==false || theHostName==null)
    	{
    		return "Rya not connected.";
    	}
    	else {
    	theHostName=null;
		full_path=null;
		connectStatus=false;
    	}
		return "Rya Disconnected..";
    }
    
    @CliCommand(value = DISCONNECT_MONGO_INSTANCE_CMD, help = "Disconnect from a specific MongoRya instance")
    public String diconnectToMongoInstance() {
    	if(connectStatusMongo==false)
    	{
    		return "MongoRya not connected.";
    	}
    	else {
    		 mongoc.closeAllConnection();
    		 connectStatusMongo=false;
    		 conn=null;
    		 mongo_times_connected=mongo_times_connected+1;
    	}
    	return "MongoRya Disconnected..";
    }
    
    
    
    
    @CliCommand(value = CONNECT_INSTANCE_CMD, help = "Connect to a specific Rya instance")
    public String connectToInstance(
            @CliOption(key = {"host"}, mandatory = true, help = "The Rya instance hostname or ip-address.")
            final String host) {
    		if(host.length()<2)
    		{
    			full_path=null;
    			connectStatus=false;
    		}
    		else
    		{
    			theHostName=host;
    			
    			//Validate connection
    			try {
    			full_path="http://"+theHostName+":8080/web.rya/sparqlQuery.jsp";
    			HttpURLConnection huc=(HttpURLConnection) new URL(full_path).openConnection();
    			huc.setReadTimeout(2000);
    			huc.setConnectTimeout(2000);
    			huc.setRequestMethod("GET");
    			responseCode=huc.getResponseCode();
    			if(responseCode==200) {
    			connectStatus=true;}
    			else { connectStatus=false; }
    			} catch (Exception e) {
    				//Connection failed.
    				connectStatus=false;
    			} 
    		}
    		if(connectStatus==true)
    		{
    			connectedOutputStatus="Rya Connected: "+theHostName;
    		}
    		else {
    			connectedOutputStatus="Rya Connect Failure..";
    		}
    		return connectedOutputStatus;
    }
    
    @CliCommand(value = CONNECT_MONGO_INSTANCE_CMD, help = "Connect to a specific MongoRya instance")
    public String connectToMongoInstance(
            @CliOption(key = {"uname"}, mandatory = true, help = "The MongoRya instance hostname or ip-address.")
            final String username,
            @CliOption(key = {"passwd"}, mandatory = true, help = "The MongoRya instance hostname or ip-address.")
            final String password,
            @CliOption(key = {"dspecs"}, mandatory = true, help = "The MongoRya instance hostname or ip-address.")
            final String dbSpecs) {
    	//mongoURL:mongoPort:dbName:mPrefix
    	//String dbSpecs="localhost:27017:rya:rya_";
    	//boolean conn_status=mongoc.establishConnection(username,password,dbSpecs);
       	if(connectStatusMongo==true)
    	{
    		return "MongoRya already connected.";
    	}
       	else if(mongo_times_connected!=0) {
       		return "Exit the shell to reconnect to MongoRya.";
       	}
    	else {
    		connectStatusMongo=mongoc.establishConnection(username,password,dbSpecs);
    	}
       	if(connectStatusMongo==false) {
       		return "MongoRya connection failed. MongoDB down or bad credentials.";
       	}

       		return "MongoRya connection success.";
    }
    
    @CliCommand(value = QUERY_MONGO_RYA_CMD, help = "Query MongoRya instance")
    public String queryMonogRyaInstance(
            @CliOption(key = {"qr"}, mandatory = true, help = "Pass Query for MongoRya.")
            final String query, 
    		@CliOption(key = {"type"}, mandatory = true, help = "Query type for MongoRya[insert, delete, update, or rquery].")
    		final String theType,
    		@CliOption(key = {"rpattern"}, mandatory = true, help = "Return rdf pattern for MongoRya[subject:object:...].")
    		final String thePattern) {
    		query_result=null;
    		if(connectStatusMongo==false)
    		{
    			return "MongoRya not connected.";
    		}else {
    			if(theType.equals("insert")==true || theType.equals("delete")==true || theType.equals("update")==true) {
    				conn=mongoc.getConnector();
    				try {
    				queryInsDelUpdatMongo(conn,query);
    				} finally {
    					return "MongoRya query completed."; }
    				}
    			else if(theType.equals("rquery")==true)
    			{
    				if(thePattern.length()<1) {
    					return "MongoRya query invalid rpattern.";
    				}else {
    					conn=mongoc.getConnector();
    					try {
							query_result=queryMongo(conn, thePattern, query);
						} catch (TupleQueryResultHandlerException e) {
							return "MongoRya query failed.";
						} catch (MalformedQueryException e) {
							
							return "MongoRya query failed.";
						} catch (RepositoryException e) {
							
							return "MongoRya query failed.";
						} catch (UpdateExecutionException e) {
							
							return "MongoRya query failed.";
						} catch (QueryEvaluationException e) {
							
							return "MongoRya query failed.";
						}
    				}
    			}else {
    				return "MongoRya query invalid type.";
    			}
    			
    		}
    	
    	return query_result;
    }
    
    @CliCommand(value = QUERY_RYA_CMD, help = "Query Rya instance")
    public String queryInstance(
            @CliOption(key = {"qr"}, mandatory = true, help = "Pass Query for Rya.")
            final String query, 
    		@CliOption(key = {"frmt"}, mandatory = true, help = "Query output format for Rya[xml or json].")
    		final String qformat) {
    		if(full_path==null || connectStatus==false)
    		{ query_result="Rya not connected.."; }
    		else {
    			try {
    			//Execute the query
    			query_result="";
    			String queryenc=URLEncoder.encode(query,"UTF-8");
				String the_qr="";
				if(qformat.equals("xml"))
				{
					the_qr=full_path.replace("sparqlQuery.jsp", "queryrdf?query.resultformat=xml&query="+queryenc);
				}
				else if(qformat.equals("json"))
				{
					the_qr=full_path.replace("sparqlQuery.jsp", "queryrdf?query.resultformat=json&query="+queryenc);
				}
				else {
					the_qr=full_path.replace("sparqlQuery.jsp", "queryrdf?query="+queryenc); 
				}
				URL url=new URL(the_qr);
				URLConnection urlConnection=url.openConnection();
				urlConnection.setDoOutput(true);
			
				BufferedReader rd=new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
				String line;
				while((line=rd.readLine())!=null) {
					query_result=query_result+line+"\n";
					
				}
				rd.close();
    			} catch (Exception e) {
    				//Connection failed.
    				query_result="Query failed..";
    			} 
    		}
    		
    		return query_result;
    }
    
    
    
    public static String queryMongo(final SailRepositoryConnection conn, String pattern, String theQuery) throws MalformedQueryException, RepositoryException,
    UpdateExecutionException, QueryEvaluationException, TupleQueryResultHandlerException {
    	
    	String output_result="";
    	String[] pattern_split=pattern.split(":");
    	String temp_str=null;
    	String temp_pattern_detect=null;
    	ArrayList<String> obj_list=null;
    	obj_list = new ArrayList<String>();
    	int pat_i=0;
    	int tab_cnt=0;
    	if(pattern_split.length<1)
    	{
    		return null;
    	}
    	
    	for(pat_i=0; pat_i<pattern_split.length; pat_i++)
    	{
    		temp_str=pattern_split[pat_i];
    		temp_pattern_detect=" ?"+temp_str+" ";
    		
    		if(temp_str.length()<1)
    		{
    			return null;
    		}
    		else if(theQuery.contains(temp_pattern_detect)==false)
    		{
    			return null;
    		}
    		else if(theQuery.contains("UPDATE ")==true || theQuery.contains("update ")==true || theQuery.contains("DELETE ")==true || theQuery.contains("delete ")==true || theQuery.contains("INSERT ")==true || theQuery.contains("insert ")==true) {
    			return null;
    		}
    		else {
    			
    			obj_list.add(temp_str);
    		}
    	}
    	TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, theQuery);
        TupleQueryResult theResult =tupleQuery.evaluate();
        while(theResult.hasNext()) {
            BindingSet s = theResult.next();
            //System.out.println("The size is: "+pattern_split.length);
            for(pat_i=0; pat_i<obj_list.size(); pat_i++)
            {
            	temp_str=obj_list.get(pat_i);
            	//System.out.println("The object: "+temp_str);
            	for(tab_cnt=0; tab_cnt<pat_i; tab_cnt++)
            	{
            		output_result=output_result+"\t";
            	}
            	Value object=s.getValue(temp_str);
            	output_result=output_result+" ?"+temp_str+" "+object+"\n";
            }
        }
    	
    	return output_result;
    }
    
    public static void queryInsDelUpdatMongo(final SailRepositoryConnection conn, String theQuery) throws MalformedQueryException, RepositoryException,
    UpdateExecutionException, QueryEvaluationException, TupleQueryResultHandlerException {
    	
        Update update = conn.prepareUpdate(QueryLanguage.SPARQL, theQuery);
        update.execute();
    }
    
}