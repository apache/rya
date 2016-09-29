/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package mvm.rya.indexing.entity.query;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Set;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.indexing.entity.model.Type;
import mvm.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

/**
 * TODO doc
 */
public class EntityNode extends ExternalSet implements ExternalBatchingIterator {

    private final Type type;
    private final Set<StatementPattern> patterns;

    // the type defines what kind of entity this node matches over
    // the patterns are the part of the query that need to be filled in for the type.

    /**
     * Constructs an instance of {@link EntityNode}.
     *
     * @param type - The type of {@link Entity} this node matches. (not null)
     * @param patterns - The query StatementPatterns that are solved using an
     *   Entity of the Type. (not null)
     */
    public EntityNode(Type type, Set<StatementPattern> patterns) {
        requireNonNull(type);
        requireNonNull(patterns);

        // TODO Preconditions.
        // All of the Subjects within the patterns must be either all the same variable or all the same constant value.
        // All of the predicates of the patterns must be constants and match the Type's property names.
        // The SPs must have a single SP that specifies the Type's ID and it must match the Type object.


        this.type = type;
        this.patterns = patterns;

        // TODO Also, map each variable that is in an Object spot each variable can be mapped to a property name as well

        // Processing note:
        // Any constant that appears in the Object portion of the SP will be used to make sure they match.
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(Collection<BindingSet> bindingset) throws QueryEvaluationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bs) throws QueryEvaluationException {

        // RETURN AN EMPTY ITERATION IF IT CAN NOT FILL IT IN!

        // for all variables in the OBJECT portion of the SPs, fill 'em in using the entity that is stored in the index.

        // x = alice's SSN
        // y = blue  <-- how do i know this is for urn:eye property? FROM THE STATEMENT PATTERN. look for the ?y in the SP, and that has the property name in it.

        // TODO Auto-generated method stub
        return null;
    }
}