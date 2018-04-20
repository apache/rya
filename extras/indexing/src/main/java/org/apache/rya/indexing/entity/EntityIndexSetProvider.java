/*
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
package org.apache.rya.indexing.entity;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Provides {@link EntityQueryNodes}s.
 */
public class EntityIndexSetProvider implements ExternalSetProvider<EntityQueryNode> {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private Multimap<Type, StatementPattern> typeMap;
    private Map<String, Type> subjectTypeMap;
    private final TypeStorage typeStorage;
    private final EntityStorage entityStorage;

    /**
     * Creates a new {@link EntityIndexSetProvider}.
     * @param typeStorage - The mechanism for access the various {@link Type}s. (not null)
     * @param entityStorage - The mechanism for access to the various {@link Entity}s. (not null)
     */
    public EntityIndexSetProvider(final TypeStorage typeStorage, final EntityStorage entityStorage) {
        this.typeStorage = requireNonNull(typeStorage);
        this.entityStorage = requireNonNull(entityStorage);
    }

    private Var getTypeSubject(final Type type) {
        //we just need the first pattern since all the patterns in this type map are the same subject
        final StatementPattern pattern = typeMap.get(type).iterator().next();
        return pattern.getSubjectVar();
    }

    private RyaIRI getPredIRI(final StatementPattern pattern) {
        final Var pred = pattern.getPredicateVar();
        return new RyaIRI(pred.getValue().stringValue());
    }

    @Override
    public List<EntityQueryNode> getExternalSets(final QuerySegment<EntityQueryNode> node) {
        typeMap = HashMultimap.create();
        subjectTypeMap = new HashMap<>();

        //discover entities
        final List<StatementPattern> unused = new ArrayList<>();
        for (final QueryModelNode pattern : node.getOrderedNodes()) {
            if(pattern instanceof StatementPattern) {
                discoverEntities((StatementPattern) pattern, unused);
            }
        }

        final List<EntityQueryNode> nodes = new ArrayList<>();
        for(final Type type : typeMap.keySet()) {
            //replace all nodes in the tupleExpr of the collection of statement patterns with this node.
            final EntityQueryNode entity = new EntityQueryNode(type, typeMap.get(type), entityStorage);
            nodes.add(entity);
        }
        return nodes;
    }

    private void discoverEntities(final StatementPattern pattern, final List<StatementPattern> unmatched) {
        final Var subj = pattern.getSubjectVar();
        final String subjStr = subj.getName();
        final RyaIRI predIRI = getPredIRI(pattern);
        //check to see if current node is type
        if(VF.createIRI(predIRI.getData()).equals(RDF.TYPE)) {
            final Var obj = pattern.getObjectVar();
            final RyaIRI objURI = new RyaIRI(obj.getValue().stringValue());
            try {
                final Optional<Type> optType = typeStorage.get(objURI);
                //if is type, fetch type add to subject -> type map
                if(optType.isPresent()) {
                    final Type type = optType.get();
                    typeMap.put(type, pattern);
                    subjectTypeMap.put(subjStr, type);
                    //check unmatched properties, add matches
                    for(final StatementPattern propertyPattern : unmatched) {
                        //store sps into the type -> property map
                        final RyaIRI property = getPredIRI(propertyPattern);
                        final Var typeSubVar = getTypeSubject(type);
                        final Var patternSubVar = propertyPattern.getSubjectVar();
                        if (type.getPropertyNames().contains(property) && typeSubVar.equals(patternSubVar)) {
                            typeMap.put(type, propertyPattern);
                        }
                    }
                }
            } catch (final TypeStorageException e) {
                e.printStackTrace();
            }
        } else {
            //if not type, check to see if subject is in type map
            if(subjectTypeMap.containsKey(subjStr)) {
                //if is, check to see if pred is a property of type
                final Type type = subjectTypeMap.get(subjStr);
                if(type.getPropertyNames().contains(predIRI)) {
                    //if is, add sp to type -> sp map
                    if(!typeMap.containsKey(type)) {
                        //each variable can only contain 1 type for now @see:Rya-235?
                        typeMap.put(type, pattern);
                    }
                } else {
                    //if not, add to unmatched type
                    unmatched.add(pattern);
                }
            } else {
                //if not, add to unmatched
                unmatched.add(pattern);
            }
        }
    }

    @Override
    public Iterator<List<EntityQueryNode>> getExternalSetCombos(final QuerySegment<EntityQueryNode> segment) {
        final List<List<EntityQueryNode>> comboList = new ArrayList<>();
        comboList.add(getExternalSets(segment));
        return comboList.iterator();
    }
}
