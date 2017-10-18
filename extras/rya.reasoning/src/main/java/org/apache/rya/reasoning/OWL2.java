package org.apache.rya.reasoning;

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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;

/**
 * Some useful OWL 2 URIs not in RDF4J API.
 */
public class OWL2 {
    private static IRI uri(String local) {
        return SimpleValueFactory.getInstance().createIRI(OWL.NAMESPACE, local);
    }
    public static final IRI ASYMMETRICPROPERTY = uri("AsymmetricProperty");
    public static final IRI IRREFLEXIVEPROPERTY = uri("IrreflexiveProperty");
    public static final IRI PROPERTYDISJOINTWITH = uri("propertyDisjointWith");
    public static final IRI ONCLASS = uri("onClass");
    public static final IRI MAXQUALIFIEDCARDINALITY = uri("maxQualifiedCardinality");
}
