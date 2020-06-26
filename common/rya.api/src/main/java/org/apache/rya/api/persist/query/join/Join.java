package org.apache.rya.api.persist.query.join;

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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.persist.RyaDAOException;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import java.util.Map;

/**
 * Date: 7/24/12
 * Time: 4:28 PM
 */
public interface Join<C extends RdfCloudTripleStoreConfiguration> {

    /**
     * Returns the {@link RyaStatement}s matching a predicate.
     */
    public CloseableIteration<RyaStatement, RyaDAOException> join(C conf, RyaIRI... preds)
            throws RyaDAOException;

    /**
     * Returns the subject given a predicate and object.
     */
    public CloseableIteration<RyaResource, RyaDAOException> join(C conf, Map.Entry<RyaIRI, RyaValue>... predObjs)
                    throws RyaDAOException;
}
