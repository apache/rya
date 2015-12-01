package mvm.rya.indexing.mongodb;

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


import java.io.IOException;
import java.util.Collection;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.index.RyaSecondaryIndexer;

import org.apache.hadoop.conf.Configuration;

public abstract class AbstractMongoIndexer implements RyaSecondaryIndexer {

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush() throws IOException {
    }


    @Override
    public Configuration getConf() {
        return null;
    }


    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void storeStatements(Collection<RyaStatement> ryaStatements)
            throws IOException {
        for (RyaStatement ryaStatement : ryaStatements){
            storeStatement(ryaStatement);
        }
        
    }

    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGraph(RyaURI... graphs) {
        throw new UnsupportedOperationException();
    }

}
