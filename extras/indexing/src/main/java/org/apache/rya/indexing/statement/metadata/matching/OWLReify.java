package org.apache.rya.indexing.statement.metadata.matching;
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
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;

public class OWLReify {

    /** http://www.w3.org/2002/07/owl#Annotation*/
    public final static IRI ANNOTATION;

    /** http://www.w3.org/2002/07/owl#annotatedSource*/
    public static final IRI SOURCE;
    
    /** http://www.w3.org/2002/07/owl#annotatedProperty*/
    public static final IRI PROPERTY;
    
    /** http://www.w3.org/2002/07/owl#annotatedTarget*/
    public static final IRI TARGET;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();
        ANNOTATION = factory.createIRI(OWL.NAMESPACE, "Annotation");
        PROPERTY = factory.createIRI(OWL.NAMESPACE, "annotatedProperty");
        SOURCE = factory.createIRI(OWL.NAMESPACE, "annotatedSource");
        TARGET = factory.createIRI(OWL.NAMESPACE, "annotatedTarget");
    }
}
    
    

