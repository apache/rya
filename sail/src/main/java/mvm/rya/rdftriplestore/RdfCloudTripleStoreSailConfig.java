package mvm.rya.rdftriplestore;

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



import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailImplConfigBase;

public class RdfCloudTripleStoreSailConfig extends SailImplConfigBase {
    
    public static final String NAMESPACE = "http://www.openrdf.org/config/sail/cloudbasestore#";

	public static final URI SERVER;
	public static final URI PORT;
	public static final URI INSTANCE;
	public static final URI USER;
	public static final URI PASSWORD;

    static {
		ValueFactory factory = ValueFactoryImpl.getInstance();
		SERVER = factory.createURI(NAMESPACE, "server");
		PORT = factory.createURI(NAMESPACE, "port");
		INSTANCE = factory.createURI(NAMESPACE, "instance");
		USER = factory.createURI(NAMESPACE, "user");
		PASSWORD = factory.createURI(NAMESPACE, "password");
	}

	private String server = "stratus13";

	private int port = 2181;

	private String user = "root";

	private String password = "password";
	
	private String instance = "stratus";

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

    @Override
	public void parse(Graph graph, Resource implNode)
		throws SailConfigException
	{
		super.parse(graph, implNode);
        System.out.println("parsing");

		try {
			Literal serverLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, SERVER);
			if (serverLit != null) {
				setServer(serverLit.getLabel());
			}
			Literal portLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, PORT);
			if (portLit != null) {
				setPort(Integer.parseInt(portLit.getLabel()));
			}
			Literal instList = GraphUtil.getOptionalObjectLiteral(graph, implNode, INSTANCE);
			if (instList != null) {
				setInstance(instList.getLabel());
			}
			Literal userLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, USER);
			if (userLit != null) {
				setUser(userLit.getLabel());
			}
			Literal pwdLit = GraphUtil.getOptionalObjectLiteral(graph, implNode, PASSWORD);
			if (pwdLit != null) {
				setPassword(pwdLit.getLabel());
			}
		}
		catch (GraphUtilException e) {
			throw new SailConfigException(e.getMessage(), e);
		}
	}
}
