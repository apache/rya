package org.apache.rya.indexing;

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


import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.apache.rya.indexing.accumulo.entity.StarQuery;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

public interface DocIdIndexer extends Closeable {

  

    public abstract CloseableIteration<BindingSet, QueryEvaluationException> queryDocIndex(StarQuery query,
            Collection<BindingSet> constraints) throws TableNotFoundException, QueryEvaluationException;

   

    @Override
    public abstract void close() throws IOException;
    
}
