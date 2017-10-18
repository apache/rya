package org.apache.rya.api.persist;

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

import java.util.List;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

/**
 * Class RdfEvalStatsDAO
 * Date: Feb 28, 2012
 * Time: 4:17:05 PM
 */
public interface RdfEvalStatsDAO<C extends RdfCloudTripleStoreConfiguration> {
    enum CARDINALITY_OF {
        SUBJECT, PREDICATE, OBJECT, SUBJECTPREDICATE, SUBJECTOBJECT, PREDICATEOBJECT
    }

    void init() throws RdfDAOException;

    boolean isInitialized() throws RdfDAOException;

    void destroy() throws RdfDAOException;

    // XXX returns -1 if no cardinality could be found.
    double getCardinality(C conf, CARDINALITY_OF card, List<Value> val) throws RdfDAOException;
	double getCardinality(C conf, CARDINALITY_OF card, List<Value> val, Resource context) throws RdfDAOException;

    void setConf(C conf);

    C getConf();

}
