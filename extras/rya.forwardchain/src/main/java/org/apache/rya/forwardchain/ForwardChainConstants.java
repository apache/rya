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
package org.apache.rya.forwardchain;

import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;

public class ForwardChainConstants {
    private static final ValueFactory VF = RdfCloudTripleStoreConstants.VALUE_FACTORY;
    private static final String NAMESPACE = RyaSchema.NAMESPACE;

    public static final IRI DERIVATION_TIME = VF.createIRI(NAMESPACE, "forwardChainIteration");
    public static final IRI DERIVATION_RULE = VF.createIRI(NAMESPACE, "forwardChainRule");

    public static final RyaURI RYA_DERIVATION_RULE = RdfToRyaConversions.convertURI(DERIVATION_RULE);
    public static final RyaURI RYA_DERIVATION_TIME = RdfToRyaConversions.convertURI(DERIVATION_TIME);
}
