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
package org.apache.rya.forwardchain.rule;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.domain.VarNameUtils;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.forwardchain.ForwardChainConstants;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.strategy.AbstractRuleExecutionStrategy;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SP;
import org.eclipse.rdf4j.model.vocabulary.SPIN;
import org.eclipse.rdf4j.query.AbstractTupleQueryResultHandler;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Represents a SPIN Construct rule extracted from the data store, providing
 * access to its associated query tree and providing methods to apply the rule.
 */
public class SpinConstructRule extends AbstractConstructRule {
    private static Logger logger = Logger.getLogger(SpinConstructRule.class);

    private final Resource ruleId;
    private final ParsedGraphQuery graphQuery;
    private Set<StatementPattern> antecedentStatementPatterns = null;
    private Set<StatementPattern> consequentStatementPatterns = null;

    /**
     * Instantiate a SPIN construct rule given its associated type, URI or bnode
     * identifier, and construct query tree. Modifies the query tree to
     * incorporate the fact that ?this must belong to the associated type, and
     * traverses the modified tree to find antecedent and consequent triple
     * patterns.
     * @param type This rule applies to objects of this type. Should not be
     *  null. If the type is owl:Thing or rdfs:Resource, it will be applied to
     *  any objects. Otherwise, a statement pattern will be added that
     *  effectively binds ?this to members of the type. Therefore, passing
     *  owl:Thing or rdfs:Resource yields the intended behavior of
     *  sp:thisUnbound.
     * @param ruleId The Resource representing this rule in the RDF data;
     *  should not be null.
     * @param graphQuery The query tree corresponding to the "construct" text;
     *  should not be null.
     */
    public SpinConstructRule(Resource type, Resource ruleId,
            ParsedGraphQuery graphQuery) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(ruleId);
        Preconditions.checkNotNull(graphQuery);
        this.ruleId = ruleId;
        this.graphQuery = graphQuery;
        // Add the type requirement: ?this must belong to the type
        graphQuery.getTupleExpr().visit(new TypeRequirementVisitor("this", type));
        // Find all statement patterns that could trigger this rule
        AntecedentVisitor aVisitor = new AntecedentVisitor();
        graphQuery.getTupleExpr().visit(aVisitor);
        antecedentStatementPatterns = aVisitor.getAntecedents();
        // Construct statement patterns for all possible conclusions of this rule
        ConstructConsequentVisitor cVisitor = new ConstructConsequentVisitor();
        graphQuery.getTupleExpr().visit(cVisitor);
        consequentStatementPatterns = cVisitor.getConsequents();
    }

    /**
     * Get the URI or bnode associated with this rule in the data.
     * @return The rule's identifier.
     */
    public Resource getId() {
        return ruleId;
    }

    @Override
    public String toString() {
        return "SpinConstructRule{" + ruleId.stringValue() + "}";
    }

    @Override
    public ParsedGraphQuery getQuery() {
        return graphQuery;
    }

    @Override
    public boolean canConclude(StatementPattern sp) {
        Preconditions.checkNotNull(sp);
        Value s1 = getVarValue(sp.getSubjectVar());
        Value p1 = getVarValue(sp.getPredicateVar());
        Value o1 = getVarValue(sp.getObjectVar());
        Value c1 = getVarValue(sp.getContextVar());
        for (StatementPattern consequent : consequentStatementPatterns) {
            Value s2 = getVarValue(consequent.getSubjectVar());
            Value p2 = getVarValue(consequent.getPredicateVar());
            Value o2 = getVarValue(consequent.getObjectVar());
            Value c2 = getVarValue(consequent.getContextVar());
            if ((s1 == null || s2 == null || s1.equals(s2))
                    && (p1 == null || p2 == null || p1.equals(p2))
                    && (o1 == null || o2 == null || o1.equals(o2))
                    && (c1 == null || c2 == null || c1.equals(c2))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Collection<StatementPattern> getAntecedentPatterns() {
        return antecedentStatementPatterns;
    }

    @Override
    public Collection<StatementPattern> getConsequentPatterns() {
        return consequentStatementPatterns;
    }

    @Override
    public long execute(AbstractRuleExecutionStrategy strategy,
            StatementMetadata metadata) throws ForwardChainException {
        metadata.addMetadata(ForwardChainConstants.RYA_DERIVATION_RULE,
                RdfToRyaConversions.convertResource(ruleId));
        return super.execute(strategy, metadata);
    }

    private static Value getVarValue(Var var) {
        return var == null ? null : var.getValue();
    }

    private static class TypeRequirementVisitor extends AbstractQueryModelVisitor<RuntimeException> {
        private static final Var RDF_TYPE_VAR = VarNameUtils.createUniqueConstVar(RDF.TYPE);
        private static final Set<Resource> BASE_TYPES = Sets.newHashSet(RDFS.RESOURCE, OWL.THING);
        static {
            RDF_TYPE_VAR.setConstant(true);
        }

        private final String varName;
        private final StatementPattern typeRequirement;
        public TypeRequirementVisitor(String varName, Resource requiredType) {
            final Var typeVar = VarNameUtils.createUniqueConstVar(requiredType);
            typeVar.setConstant(true);
            this.varName = varName;
            if (BASE_TYPES.contains(requiredType)) {
                this.typeRequirement = null;
            }
            else {
                this.typeRequirement = new StatementPattern(new Var(varName), RDF_TYPE_VAR, typeVar);
            }
        }
        @Override
        public void meet(SingletonSet node) {
            if (typeRequirement != null) {
                node.replaceWith(typeRequirement);
            }
        }
        @Override
        public void meet(Extension node) {
            Set<String> argBindings = node.getArg().getBindingNames();
            if (typeRequirement != null) {
                node.getElements().removeIf(elem -> {
                    if (varName.equals(elem.getName())) {
                        ValueExpr expr = elem.getExpr();
                        if (expr == null) {
                            return true;
                        }
                        else if (expr instanceof Var) {
                            String fromName = ((Var) expr).getName();
                            if (getVarValue((Var) expr) == null && !argBindings.contains(fromName)) {
                                return true;
                            }
                        }
                    }
                    return false;
                });
                meetUnaryTupleOperator(node);
            }
        }
        @Override
        public void meetNode(QueryModelNode node) {
            if (typeRequirement != null) {
                if (node instanceof TupleExpr && ((TupleExpr) node).getBindingNames().contains(varName)) {
                    final Join withType = new Join((TupleExpr) node.clone(), typeRequirement);
                    node.replaceWith(withType);
                }
                else {
                    node.visitChildren(this);
                }
            }
        }
        @Override
        public void meetUnaryTupleOperator(UnaryTupleOperator node) {
            if (typeRequirement != null) {
                if (node.getArg().getBindingNames().contains(varName)) {
                    node.visitChildren(this);
                }
                else {
                    meetNode(node);
                }
            }
        }
    }

    /**
     * Load a set of SPIN rules from a data store.
     * @param conf Contains the connection information. Not null.
     * @return A map of rule identifiers to rule objects.
     * @throws ForwardChainException if connecting, querying for rules, or
     *  parsing rules fails.
     */
    public static Ruleset loadSpinRules(RdfCloudTripleStoreConfiguration conf)
            throws ForwardChainException {
        Preconditions.checkNotNull(conf);
        Map<Resource, Rule> rules = new ConcurrentHashMap<>();
        // Connect to Rya
        SailRepository repository = null;
        SailRepositoryConnection conn = null;
        try {
            repository = new SailRepository(RyaSailFactory.getInstance(conf));
        } catch (Exception e) {
            throw new ForwardChainException("Couldn't initialize SAIL from configuration", e);
        }
        // Load and parse the individual SPIN rules from the data store
        String ruleQueryString = "SELECT ?type ?rule ?text WHERE {\n"
                + "  ?type <" + SPIN.RULE_PROPERTY.stringValue() + "> ?rule .\n"
                + "  {\n"
                + "    ?rule a <" + SP.CONSTRUCT_CLASS.stringValue() + "> .\n"
                + "    ?rule <" + SP.TEXT_PROPERTY.stringValue() + "> ?text .\n"
                + "  } UNION {\n"
                + "    ?rule a ?template .\n"
                + "    ?template <" + SPIN.BODY_PROPERTY + ">? ?body .\n"
                + "    ?body a <" + SP.CONSTRUCT_CLASS.stringValue() + "> .\n"
                + "    ?body <" + SP.TEXT_PROPERTY.stringValue() + "> ?text .\n"
                + "  }\n"
                + "}";
        SPARQLParser parser = new SPARQLParser();
        try {
            conn = repository.getConnection();
            TupleQuery ruleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, ruleQueryString);
            ruleQuery.evaluate(new AbstractTupleQueryResultHandler() {
                @Override
                public void handleSolution(BindingSet bs) throws TupleQueryResultHandlerException {
                // For each rule identifier found, instantiate a SpinRule
                    Value requiredType = bs.getValue("type");
                    Value ruleIdentifier = bs.getValue("rule");
                    Value ruleText = bs.getValue("text");
                    if (requiredType instanceof Resource
                            && ruleIdentifier instanceof Resource
                            && ruleText instanceof Literal) {
                        ParsedQuery parsedRule;
                        try {
                            parsedRule = parser.parseQuery(ruleText.stringValue(), null);
                            if (parsedRule instanceof ParsedGraphQuery) {
                                SpinConstructRule rule = new SpinConstructRule(
                                        (Resource) requiredType,
                                        (Resource) ruleIdentifier,
                                        (ParsedGraphQuery) parsedRule);
                                if (rule.hasAnonymousConsequent()) {
                                    logger.error("Skipping unsupported rule " + ruleIdentifier
                                            + " -- consequent refers to bnode, which is not"
                                            + " currently supported (creating new bnodes at each"
                                            + " application could lead to infinite recursion).");
                                }
                                else {
                                    rules.put((Resource) ruleIdentifier, rule);
                                }
                            }
                        } catch (Exception e) {
                            throw new TupleQueryResultHandlerException(e);
                        }
                    }
                }
            });
        } catch (TupleQueryResultHandlerException | QueryEvaluationException
                | MalformedQueryException | RepositoryException e) {
            throw new ForwardChainException("Couldn't retrieve SPIN rules", e);
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (RepositoryException e) {
                    logger.warn("Error closing repository connection", e);
                }
            }
            if (repository.isInitialized()) {
                try {
                    repository.shutDown();
                } catch (RepositoryException e) {
                    logger.warn("Error shutting down repository", e);
                }
            }
        }
        return new Ruleset(rules.values());
    }
}
