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
package org.apache.rya.rdftriplestore.inference;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 * Expands the query tree to account for any relevant has-self class expressions
 * in the ontology known to the {@link InferenceEngine}.
 *
 * Only operates on {@link StatementPattern} nodes, and only those including a
 * defined type or defined predicate which is relevant to a has-self expression
 * in the ontology. When applicable, replaces the node with one or more
 * {@link InferUnion}s, one of whose leaves is the original StatementPattern.
 *
 * A has-self restriction defines the set of resources that are connected to
 * themselves by a property. If the ontology states that a type is a resource
 * that has a self referencing property, then the inference engine should:
 * <li>1. Rewrite queries of the from ?x rdf:type :T to find all resources
 * matching ?x :P ?x (as well as anything explicitly stated to be of type :T)
 * </li>
 * <li>2. Rewrite queries of the from :CONST :P ?o or ?subj :P :CONST to match
 * :CONST if :CONST is known to have the type :T
 * </li>
 */
public class HasSelfVisitor extends AbstractInferVisitor {
    private static final Var TYPE_VAR = new Var(RDF.TYPE.stringValue(), RDF.TYPE);

    /**
     * Creates a new {@link HasSelfVisitor}.
     * @param conf The {@link RdfCloudTripleStoreConfiguration}.
     * @param inferenceEngine The InferenceEngine containing the relevant ontology.
     */
    public HasSelfVisitor(final RdfCloudTripleStoreConfiguration conf, final InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferHasSelf();
    }

    @Override
    protected void meetSP(final StatementPattern node) throws Exception {
        final URI pred = (URI) node.getPredicateVar().getValue();
        final Var obj = node.getObjectVar();
        //if originalSP like (?s rdf:type :C1):  require that C1 is defined, i.e. not a variable
        // node <- originalSP
        final StatementPattern clone = node.clone();
        if (RDF.TYPE.equals(pred) && obj.isConstant()) {
            //for property in getHasSelfImplyingType(C1):
            if (obj.getValue() instanceof URI) {
                for (final URI property : inferenceEngine.getHasSelfImplyingType((URI) obj.getValue())) {
                    //node <- InferUnion(node, StatementPattern(?s, property, ?s)).
                    final InferUnion union = new InferUnion(clone,
                            new StatementPattern(clone.getSubjectVar(),
                                                 new Var(property.stringValue(), property),
                                                 clone.getSubjectVar()));
                    //originalSP.replaceWith(node)
                    node.replaceWith(union);
                }
            }
        //else if originalSP like (s :p o):  where p is not a variable and at least one of s and o are variables
        } else if (node.getPredicateVar().isConstant()
               && (!node.getSubjectVar().isConstant() ||
                   !node.getObjectVar().isConstant())) {
            //for type in getHasSelfImplyingProperty(p):
            for (final Resource type : inferenceEngine.getHasSelfImplyingProperty(pred)) {
                final Extension extension;
                if(obj.isConstant()) { // subject is the variable
                    //Extension(StatementPattern(o, rdf:type, type), ExtensionElem(o, "s"))
                    extension = new Extension(
                            new StatementPattern(obj, TYPE_VAR, new Var(type.stringValue(), type)),
                            new ExtensionElem(obj, node.getSubjectVar().getName()));
                } else { //o is a variable and s may either be defined or a variable
                    //Extension(StatementPattern(s, rdf:type, type), ExtensionElem(s, "o"))
                    extension = new Extension(
                            new StatementPattern(node.getSubjectVar(), TYPE_VAR, new Var(type.stringValue(), type)),
                            new ExtensionElem(node.getSubjectVar(), obj.getName()));
                }
                // node <- InferUnion(node, newNode)
                final InferUnion union = new InferUnion(extension, clone);
                node.replaceWith(union);
            }
        }
    }
}