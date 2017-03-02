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
package org.apache.rya.indexing.pcj.functions.geo;

import org.eclipse.rdf4j.model.IRI;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

/**
 * Make a RDF4J Function look like an openRDF Function.
 */
class FunctionAdapter implements Function {
    private org.eclipse.rdf4j.query.algebra.evaluation.function.Function theRdf4JFunction;

    FunctionAdapter(org.eclipse.rdf4j.query.algebra.evaluation.function.Function theRdf4JFunction) {
        this.theRdf4JFunction = theRdf4JFunction;
    }

    @Override
    public String getURI() {
        return theRdf4JFunction.getURI();
    }

    @Override
    public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
        System.out.println("Evaluate: Valuefactory=" + valueFactory);
        // need a Adapter for org.eclipse.rdf4j.model.ValueFactory
        org.eclipse.rdf4j.model.ValueFactory rdf4jValueFactory = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();
        // org.eclipse.rdf4j.model.ValueFactory rdf4jValueFactory = new ValueFactoryAdapter(valueFactory);
        org.eclipse.rdf4j.model.Value rdf4jArgs[] = new org.eclipse.rdf4j.model.Value[args.length];
        for (int i = 0; i < args.length; i++) {
            Value v = args[i];
            rdf4jArgs[i] = adaptValue(v, rdf4jValueFactory);
            // System.out.println("Evaluate: Value[" + i + "]=" + v);
        }
        org.eclipse.rdf4j.model.Value v = theRdf4JFunction.evaluate(rdf4jValueFactory, rdf4jArgs);
        if (v instanceof org.eclipse.rdf4j.model.impl.BooleanLiteral)
            return valueFactory.createLiteral(((org.eclipse.rdf4j.model.impl.BooleanLiteral) v).booleanValue());
        else if (v instanceof org.eclipse.rdf4j.model.Literal) {
            org.eclipse.rdf4j.model.Literal vLiteral = (org.eclipse.rdf4j.model.Literal) v;
            org.openrdf.model.URI vType = valueFactory.createURI(vLiteral.getDatatype().stringValue());
            org.openrdf.model.Literal theReturnValue = valueFactory.createLiteral(vLiteral.getLabel(), vType);
            System.out.println("Function RETURNS:" + theReturnValue + " class:" + theReturnValue.getClass() + " rdf4j=" + v + " class:" + v.getClass());
            return theReturnValue;
        }
        //
        else
            throw new Error("Evaluate returned unsupported value, must be a Literal or boolean literal.  value=" + v);
    }

    /**
     * Convert from OpenRDF to rdf4j value used by Geo Functions.
     * 
     * @param value
     *            Must be a URIImpl, Literal or a BooleanLiteralImpl, or throws error. Ignores language.
     * @param rdf4jValueFactory
     * @return an rdf4j Literal copied from the input
     */
    public org.eclipse.rdf4j.model.Value adaptValue(Value value, org.eclipse.rdf4j.model.ValueFactory rdf4jValueFactory) {
        if (value instanceof URIImpl) {
            URIImpl uri = (URIImpl) value;
            return rdf4jValueFactory.createIRI(uri.stringValue());
        } else if (!(value instanceof Literal)) {
            throw new UnsupportedOperationException("Not supported, value must be literal type, it was: " + value.getClass() + " value=" + value);
        }
        if (value instanceof BooleanLiteralImpl) {
            BooleanLiteralImpl bl = (BooleanLiteralImpl) value;
            if (bl.booleanValue())
                return org.eclipse.rdf4j.model.impl.BooleanLiteral.TRUE;
            else
                return org.eclipse.rdf4j.model.impl.BooleanLiteral.FALSE;
        }
        final Literal literalValue = (Literal) value;
        org.eclipse.rdf4j.model.ValueFactory vf = org.eclipse.rdf4j.model.impl.SimpleValueFactory.getInstance();
        final String label = literalValue.getLabel();
        final IRI datatype = vf.createIRI(literalValue.getDatatype().stringValue());
        return vf.createLiteral(label, datatype);
    }
}