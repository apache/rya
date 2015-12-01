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



import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

public class RdfCloudTripleStoreFactory implements SailFactory {

	public static final String SAIL_TYPE = "openrdf:RdfCloudTripleStore";

	@Override
	public SailImplConfig getConfig() {
		return new RdfCloudTripleStoreSailConfig();
	}

	@Override
	public Sail getSail(SailImplConfig config) throws SailConfigException {
//		RdfCloudTripleStore cbStore = new RdfCloudTripleStore();
//		RdfCloudTripleStoreSailConfig cbconfig = (RdfCloudTripleStoreSailConfig) config;
//		cbStore.setServer(cbconfig.getServer());
//		cbStore.setPort(cbconfig.getPort());
//		cbStore.setInstance(cbconfig.getInstance());
//		cbStore.setPassword(cbconfig.getPassword());
//		cbStore.setUser(cbconfig.getUser());
//		return cbStore;
        return null; //TODO: How?
	}

	@Override
	public String getSailType() {
		return SAIL_TYPE;
	}

}
