package org.apache.rya.api.domain;

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



import org.eclipse.rdf4j.model.impl.SimpleIRI;

/**
 * A Node is an expected node in the global graph. This typing of the IRI allows us to dictate the difference between an
 * IRI that is just an Attribute on the subject vs. an IRI that is another subject Node in the global graph. It does not
 * guarantee that the subject exists, just that there is an Edge to it.
 */
public class Node extends SimpleIRI {
    public Node() {
    }

    public Node(String iriString) {
        super(iriString);
    }
}
