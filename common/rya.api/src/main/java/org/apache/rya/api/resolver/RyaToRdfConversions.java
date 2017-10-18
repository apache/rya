package org.apache.rya.api.resolver;

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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

/**
 * Date: 7/17/12
 * Time: 8:34 AM
 */
public class RyaToRdfConversions {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    public static IRI convertURI(RyaURI uri) {
        return VF.createIRI(uri.getData());
    }
    
    private static IRI convertURI(RyaType value) {
        return VF.createIRI(value.getData());
    }

    public static Literal convertLiteral(RyaType literal) {
        if (XMLSchema.STRING.equals(literal.getDataType())) {
            return VF.createLiteral(literal.getData());
        } else {
            return VF.createLiteral(literal.getData(), literal.getDataType());
        }
        //TODO: No Language support yet
    }

    public static Value convertValue(RyaType value) {
        //assuming either uri or Literal here
        return (value instanceof RyaURI || value.getDataType().equals(XMLSchema.ANYURI)) ? convertURI(value) : convertLiteral(value);
    }

    public static Statement convertStatement(RyaStatement statement) {
        assert statement != null;
        if (statement.getContext() != null) {
            return VF.createStatement(convertURI(statement.getSubject()),
                    convertURI(statement.getPredicate()),
                    convertValue(statement.getObject()),
                    convertURI(statement.getContext()));
        } else {
            return VF.createStatement(convertURI(statement.getSubject()),
                    convertURI(statement.getPredicate()),
                    convertValue(statement.getObject()));
        }
    }

}
