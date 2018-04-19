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

import org.apache.rya.api.domain.RangeURI;
import org.apache.rya.api.domain.RangeValue;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaTypeRange;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.RyaURIRange;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

/**
 * Date: 7/17/12
 * Time: 8:34 AM
 */
public class RdfToRyaConversions {

    public static RyaURI convertURI(IRI uri) {
        if (uri == null) return null;
        if (uri instanceof RangeURI) {
            RangeURI ruri = (RangeURI) uri;
            return new RyaURIRange(convertURI(ruri.getStart()), convertURI(ruri.getEnd()));
        }
        return new RyaURI(uri.stringValue());
    }

    public static RyaType convertLiteral(Literal literal) {
        if (literal == null) return null;
        if (literal.getDatatype() != null) {
            return new RyaType(literal.getDatatype(), literal.stringValue());
        }
        //no language literal conversion yet
        return new RyaType(literal.stringValue());
    }

    public static RyaType convertValue(Value value) {
        if (value == null) return null;
        //assuming either IRI or Literal here
        if(value instanceof Resource) {
            return convertResource((Resource) value);
        }
        if (value instanceof Literal) {
            return convertLiteral((Literal) value);
        }
        if (value instanceof RangeValue) {
            RangeValue rv = (RangeValue) value;
            if (rv.getStart() instanceof IRI) {
                return new RyaURIRange(convertURI((IRI) rv.getStart()), convertURI((IRI) rv.getEnd()));
            } else {
                //literal
                return new RyaTypeRange(convertLiteral((Literal) rv.getStart()), convertLiteral((Literal) rv.getEnd()));
            }
        }
        return null;
    }

    public static RyaURI convertResource(Resource subject) {
        if(subject == null) return null;
        if (subject instanceof BNode) {
            return new RyaURI(RyaSchema.BNODE_NAMESPACE + ((BNode) subject).getID());
        }
        return convertURI((IRI) subject);
    }

    public static RyaStatement convertStatement(Statement statement) {
        if (statement == null) return null;
        Resource subject = statement.getSubject();
        IRI predicate = statement.getPredicate();
        Value object = statement.getObject();
        Resource context = statement.getContext();
        return new RyaStatement(
                convertResource(subject),
                convertURI(predicate),
                convertValue(object),
                convertResource(context));
    }

}
