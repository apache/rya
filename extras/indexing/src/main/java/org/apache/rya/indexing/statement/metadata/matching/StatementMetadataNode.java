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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExternalSet;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * This class provides users with the ability to issue reified queries to Rya.
 * As opposed to a single triple representing a statement, a reified query
 * consists of a number of triples that all describe the same statement. For
 * example, instead of having the single statement (http://Bob,
 * http://worksAt,http://CoffeeShop), the reified statement representing this
 * triple would be the collection of triples: {(_blankNode, RDF.TYPE,
 * OWLReify.ANNOTATION), (_blankNode, OWLReify.SOURCE, http://Bob), (_blankNode,
 * OWLReify.PROPERTY, http://worksAt), (_blankNode, OWLReify.TARGET, http://CoffeeShop)}.
 * The advantage of expanding the statement into a collection of triples in this
 * way is that additional assertions can be made about the statement. For
 * example, we could use the triple (_blankNode, <http://createdOn>,
 * <http://date#1/2/17>) to indicate that the statement was created on 1/2/17.
 * The drawback of reification is that is it inefficient. It takes four triples
 * to specify what was originally specified with one triple. So reificiation is
 * expensive from a storage perspective. It is also expensive from a query
 * perspective in that three joins are required to evaluate a query that is
 * reduced to a single scan in non-reified form.
 *
 * This class provides Rya with the ability to issue reified queries even though
 * statements are not reified. Each {@link RyaStatement} contains a
 * {@link StatementMetadata} field that allows users to store additional
 * metadata about a given statement. When a user issues a reified query
 * (possibly containing StatementPatterns about metadata for that statement),
 * the {@link StatementPattern}s for that parsed query are used to create this
 * class. Upon construction, the StatementMetadataNode validates that the
 * collection of StatementPatterns represents a reified query, and then
 * evaluates the reified query using a single scan over a range determined by
 * the subject, predicate, and object portions of the reified query. If
 * additional metadata properties are specified in the reified query, the
 * results of the initial scan are filtered client side by comparing the user
 * specified properties with the StatementMetadata extracted from each of the
 * results. This class allows users to issue queries about RyaStatements and any
 * contextual properties without the inefficiencies associated with reification.
 *
 * @param <C>
 *            - Configuration object
 */
public class StatementMetadataNode<C extends RdfCloudTripleStoreConfiguration> extends ExternalSet
        implements ExternalBatchingIterator {

    private static final RyaIRI TYPE_ID_URI = new RyaIRI(RDF.TYPE.toString());
    private static final RyaIRI SUBJ_ID_URI = new RyaIRI(OWLReify.SOURCE.toString());
    private static final RyaIRI PRED_ID_URI = new RyaIRI(OWLReify.PROPERTY.toString());
    private static final RyaIRI OBJ_ID_URI = new RyaIRI(OWLReify.TARGET.toString());
    private static final RyaIRI STATEMENT_ID_URI = new RyaIRI(OWLReify.ANNOTATION.toString());

    private StatementPattern statement;
    private Map<RyaIRI, Var> properties;
    private final Collection<StatementPattern> patterns;
    private final List<RyaIRI> uriList = Arrays.asList(TYPE_ID_URI, SUBJ_ID_URI, PRED_ID_URI, OBJ_ID_URI);
    private final C conf;
    private Set<String> bindingNames;
    private RyaQueryEngine<C> queryEngine;

    public StatementMetadataNode(final Collection<StatementPattern> patterns, final C conf) {
        this.conf = conf;
        this.patterns = patterns;
        verifySameSubjects(patterns);
        verifyAllPredicatesAreConstants(patterns);
        final boolean correctForm = verifyHasCorrectTypePattern(patterns);
        if (!correctForm) {
            throw new IllegalArgumentException("Invalid reified StatementPatterns.");
        }
        setStatementPatternAndProperties(patterns);
    }

    /**
     * Get {@link StatementPattern}s representing the underlying reified query.
     *
     * @return Collection of StatementPatterns
     */
    public Collection<StatementPattern> getReifiedStatementPatterns() {
        return patterns;
    }

    /**
     * Verify the Subject for all of the patterns is the same and that all
     * Subjects are {@link BNode}s.
     *
     * @param patterns
     *            - The patterns to check.
     * @throws IllegalStateException
     *             If all of the Subjects are not the same.
     */
    private static void verifySameSubjects(final Collection<StatementPattern> patterns) throws IllegalStateException {
        requireNonNull(patterns);

        final Iterator<StatementPattern> it = patterns.iterator();
        final Var subject = it.next().getSubjectVar();

        while (it.hasNext()) {
            final StatementPattern pattern = it.next();
            if (!pattern.getSubjectVar().equals(subject)) {
                throw new IllegalStateException("At least one of the patterns has a different subject from the others. "
                        + "All subjects must be the same.");
            }
        }
    }

    /**
     * Verifies all of the Statement Patterns have Constants for their
     * predicates.
     *
     * @param patterns
     *            - The patterns to check. (not null)
     * @throws IllegalStateException
     *             A pattern has a variable predicate.
     */
    private static void verifyAllPredicatesAreConstants(final Collection<StatementPattern> patterns)
            throws IllegalStateException {
        requireNonNull(patterns);

        for (final StatementPattern pattern : patterns) {
            if (!pattern.getPredicateVar().isConstant()) {
                throw new IllegalStateException(
                        "The Predicate of a Statement Pattern must be constant. Pattern: " + pattern);
            }
        }
    }

    /**
     * Verifies StatementPatterns define a reified pattern with associated
     * metadata properties.
     *
     * @param patterns
     *            - The patterns to check. (not null)
     * @throws IllegalStateException
     *             No Type or the wrong Type is specified by the patterns.
     */
    public static boolean verifyHasCorrectTypePattern(final Collection<StatementPattern> patterns)
            throws IllegalStateException {
        requireNonNull(patterns);

        boolean subjFound = false;
        boolean objFound = false;
        boolean predFound = false;
        boolean statementFound = false;
        boolean valid = true;
        boolean contextSet = false;
        Var context = null;

        for (final StatementPattern pattern : patterns) {
            final RyaIRI predicate = new RyaIRI(pattern.getPredicateVar().getValue().toString());

            if (!contextSet) {
                context = pattern.getContextVar();
                contextSet = true;
            } else {
                if(context != null && !context.equals(pattern.getContextVar())) {
                    return false;
                }
            }

            if (predicate.equals(TYPE_ID_URI)) {
                final Value objectValue = pattern.getObjectVar().getValue();
                if (objectValue != null) {
                    final RyaIRI statementID = new RyaIRI(objectValue.stringValue());
                    if (statementID.equals(STATEMENT_ID_URI)) {
                        statementFound = true;
                    } else {
                        // contains more than one Statement containing TYPE_ID_URI
                        // as Predicate
                        // and STATEMENT_ID_URI as Object
                        valid = false;
                    }
                } else {
                    valid = false;
                }
            }

            if (predicate.equals(SUBJ_ID_URI)) {
                if (!subjFound) {
                    subjFound = true;
                } else {
                    // contains more than Subject SP
                    valid = false;
                }

            }

            if (predicate.equals(PRED_ID_URI)) {
                if (!predFound) {
                    predFound = true;
                } else {
                    // contains more than one Predicate SP
                    valid = false;
                }
            }

            if (predicate.equals(OBJ_ID_URI)) {
                if (!objFound) {
                    objFound = true;
                } else {
                    // contains more than one Object SP
                    valid = false;
                }
            }
        }

        return valid && statementFound && subjFound && predFound && objFound;
    }

    /**
     * Constructs a {@link StatementPattern} from the StatementPatterns
     * representing a reified query. This StatementPattern has as a subject, the
     * object of the StatementPattern containing the predicate
     * {@link RDF#SUBJECT}. This StatementPattern has as predicate, the object
     * of the StatementPattern containing the predicate {@link RDF#PREDICATE}.
     * This StatementPattern has as an object, the object of the
     * StatementPattern containing the predicate {@link RDF#OBJECT}. This method
     * also builds a map between all predicates that are not of the above type
     * and the object {@link Var}s they are associated with. This map contains
     * the user specified metadata properties and is used for comparison with
     * the metadata properties extracted from RyaStatements passed back by the
     * {@link RyaQueryEngine}.
     *
     * @param patterns
     *            - collection of patterns representing a reified query
     */
    private void setStatementPatternAndProperties(final Collection<StatementPattern> patterns) {

        final StatementPattern sp = new StatementPattern();
        final Map<RyaIRI, Var> properties = new HashMap<>();

        for (final StatementPattern pattern : patterns) {
            final RyaIRI predicate = new RyaIRI(pattern.getPredicateVar().getValue().toString());

            if (!uriList.contains(predicate)) {
                final Var objVar = pattern.getObjectVar();
                properties.put(predicate, objVar);
                continue;
            }

            if (predicate.equals(SUBJ_ID_URI)) {
                sp.setContextVar(pattern.getContextVar());
                sp.setSubjectVar(pattern.getObjectVar());
            }

            if (predicate.equals(PRED_ID_URI)) {
                sp.setPredicateVar(pattern.getObjectVar());
            }

            if (predicate.equals(OBJ_ID_URI)) {
                sp.setObjectVar(pattern.getObjectVar());
            }
        }
        this.statement = sp;
        this.properties = properties;
    }

    /**
     * This method pairs each {@link BindingSet} in the specified collection
     * with the StatementPattern constraints and issues a query to Rya using the
     * {@link RyaQueryEngine}.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset)
            throws QueryEvaluationException {
        if (bindingset.size() == 0) {
            return new EmptyIteration<>();
        }

        queryEngine = RyaQueryEngineFactory.getQueryEngine(conf);
        final Set<Map.Entry<RyaStatement, BindingSet>> statements = new HashSet<>();
        final Iterator<BindingSet> iter = bindingset.iterator();
        while (iter.hasNext()) {
            final BindingSet bs = iter.next();
            statements.add(new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(
                    getRyaStatementFromBindings(bs), bs));
        }

        final CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> iteration;
        try {
            iteration = queryEngine.queryWithBindingSet(statements, conf);
        } catch (final RyaDAOException e) {
            throw new RuntimeException(e);
        }

        return new PropertyFilterAndBindingSetJoinIteration(iteration, properties, statement);
    }

    /**
     * Uses StatementPattern constraints to form a RyaStatement, and fills in
     * any null values with {@link BindingSet} values corresponding to the
     * variable for that position.
     *
     * @param bs
     * @return RyaStatement whose values are determined by StatementPattern and
     *         BindingSet constraints
     */
    private RyaStatement getRyaStatementFromBindings(final BindingSet bs) {

        final Value subjValue = getVarValue(statement.getSubjectVar(), bs);
        final Value predValue = getVarValue(statement.getPredicateVar(), bs);
        final Value objValue = getVarValue(statement.getObjectVar(), bs);
        final Value contextValue = getVarValue(statement.getContextVar(), bs);
        RyaIRI subj = null;
        RyaIRI pred = null;
        RyaType obj = null;
        RyaIRI context = null;

        if (subjValue != null) {
            Preconditions.checkArgument(subjValue instanceof IRI);
            subj = RdfToRyaConversions.convertIRI((IRI) subjValue);
        }

        if (predValue != null) {
            Preconditions.checkArgument(predValue instanceof IRI);
            pred = RdfToRyaConversions.convertIRI((IRI) predValue);
        }

        if (objValue != null) {
            obj = RdfToRyaConversions.convertValue(objValue);
        }

        if(contextValue != null) {
            context = RdfToRyaConversions.convertIRI((IRI) contextValue);
        }
        return new RyaStatement(subj, pred, obj, context);
    }

    /**
     * Assigns BindingSet values for any {@link Var} whose {@link Value} is
     * null. Returns the {@link Value} associated with Var (if it has one),
     * otherwise returns the BindingSet Value corresponding to
     * {@link Var#getName()}. If no such Binding exits, this method returns
     * null.
     *
     * @param var
     * @param bindings
     * @return Value
     */
    private Value getVarValue(final Var var, final BindingSet bindings) {
        if (var == null) {
            return null;
        } else if (var.hasValue()) {
            return var.getValue();
        } else {
            return bindings.getValue(var.getName());
        }
    }

    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final BindingSet bindings)
            throws QueryEvaluationException {
        return evaluate(Collections.singleton(bindings));
    }

    @Override
    public boolean equals(final Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof StatementMetadataNode) {
            final StatementMetadataNode<?> meta = (StatementMetadataNode<?>) other;
            if (meta.patterns.size() != this.patterns.size()) {
                return false;
            }

            if (this.patterns.size() != meta.patterns.size()) {
                return false;
            }

            final Set<StatementPattern> thisSet = new HashSet<>(patterns);
            final Set<StatementPattern> thatSet = new HashSet<>(meta.patterns);
            return thisSet.equals(thatSet);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hashcode = 0;
        for (final StatementPattern sp : patterns) {
            hashcode += sp.hashCode();
        }
        return hashcode;
    }

    @Override
    public Set<String> getBindingNames() {
        if (bindingNames == null) {
            bindingNames = getVariableNames();
        }
        return bindingNames;
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        return getBindingNames();
    }

    @Override
    public String getSignature() {
        return "StatementMetadataNode(" + Joiner.on(",").join(getBindingNames()) + ")";
    }

    @Override
    public String toString() {
        return getSignature();
    }

    private Set<String> getVariableNames() {
        final Set<String> vars = new HashSet<>();
        for (final StatementPattern pattern : patterns) {
            for (final Var var : pattern.getVarList()) {
                if (var.getValue() == null) {
                    vars.add(var.getName());
                }
            }
        }
        return vars;
    }

    /**
     * This is an {@link CloseableIteration} class that serves a number of
     * purposes. It's primary purpose is to filter a CloseableIteration over
     * {@link Map.Entry<RyaStatement,BindingSet>} using a specified property Map
     * from {@link RyaIRI} to {@link org.eclipse.rdf4j.query.algebra.Var}. This
     * Iteration iterates over the Entries in the user specified Iteration,
     * comparing properties in the {@link StatementMetadata} Map contained in
     * the RyaStatements with the property Map for this class. If the properties
     * match, a {@BindingSet} is formed from the RyaStatement/Properties and
     * joined (if possible) with the BindingSet taken from the Map.Entry. If the
     * RyaStatement/Property BindingSet cannot be formed or joined the the Entry
     * BindingSet, the Entry in the user specified Iteration is filtered out. So
     * this class converts Iterations, filters according to the specified
     * property Map, and joins the BindingSet formed from the
     * RyaStatements/Properties with the Entry BindingSet. }.
     *
     */
    class PropertyFilterAndBindingSetJoinIteration implements CloseableIteration<BindingSet, QueryEvaluationException> {

        private final CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> statements;
        private final Map<RyaIRI, Var> properties;
        private final StatementPattern sp;
        private BindingSet next;
        private boolean hasNextCalled = false;
        private boolean hasNext = false;

        public PropertyFilterAndBindingSetJoinIteration(
                final CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> statements,
                final Map<RyaIRI, Var> properties, final StatementPattern sp) {
            this.statements = statements;
            this.properties = properties;
            this.sp = sp;
        }

        @Override
        public boolean hasNext() throws QueryEvaluationException {
            if (!hasNextCalled) {
                hasNextCalled = true;
                hasNext = false;
                Optional<BindingSet> bs;
                try {
                    bs = getNext();
                    if (bs.isPresent()) {
                        next = bs.get();
                        hasNext = true;
                    }
                    if (!hasNext) {
                        queryEngine.close();
                    }
                    return hasNext;
                } catch (RyaDAOException | IOException e) {
                    throw new QueryEvaluationException(e);
                }
            } else {
                return hasNext;
            }
        }

        @Override
        public BindingSet next() throws QueryEvaluationException {

            if (hasNextCalled) {
                if (!hasNext) {
                    throw new NoSuchElementException();
                }
                hasNextCalled = false;
                return next;
            } else {
                hasNext();
                if (!hasNext) {
                    throw new NoSuchElementException();
                }
                hasNextCalled = false;
                return next;
            }
        }

        @Override
        public void remove() throws QueryEvaluationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws QueryEvaluationException {
            try {
                statements.close();
            } catch (final RyaDAOException e) {
                throw new QueryEvaluationException(e);
            }
        }

        /**
         * Fast-forwards Iteration to next valid Entry and builds the
         * BindingSet.
         *
         * @return BindingSet
         * @throws RyaDAOException
         */
        private Optional<BindingSet> getNext() throws RyaDAOException {
            Optional<BindingSet> optionalBs = Optional.empty();
            while (statements.hasNext() && !optionalBs.isPresent()) {
                final Map.Entry<RyaStatement, BindingSet> next = statements.next();
                optionalBs = buildBindingSet(next.getKey(), next.getValue());
            }
            return optionalBs;
        }

        /**
         * Builds BindingSet from Entry if possible. Otherwise returns an empty
         * Optional if no valid BindingSet can be built. Valid BindingSet can be
         * built if this class's property Map is consistent with
         * {@link StatementMetadata} properties for the specified RyaStatement
         * and if the BindingSet built form the StatementMetadata properties can
         * be joined with specified BindingSet.
         *
         * @param statement
         *            - RyaStatement
         * @param bindingSet
         *            - BindingSet
         * @return - Optional containing BindingSet is a valid BindingSet could
         *         be built
         */
        private Optional<BindingSet> buildBindingSet(final RyaStatement statement, final BindingSet bindingSet) {

            final QueryBindingSet bs = new QueryBindingSet();
            final Optional<BindingSet> optPropBs = buildPropertyBindingSet(statement);
            if (!optPropBs.isPresent()) {
                return Optional.empty();
            }
            final BindingSet propBs = optPropBs.get();
            final BindingSet spBs = buildBindingSetFromStatementPattern(statement);
            if (!canJoinBindingSets(spBs, propBs)) {
                return Optional.empty();
            }
            bs.addAll(spBs);
            bs.addAll(propBs);
            if (!canJoinBindingSets(bs, bindingSet)) {
                return Optional.empty();
            }
            bs.addAll(bindingSet);
            return Optional.of(bs);

        }

        /**
         * Verifies whether this class's property Map is consistent with
         * StatementMetadata properties for specified RyaStatement. If
         * consistent, this method builds the associated BindingSet otherwise an
         * empty Optional is returned.
         *
         * @param statement
         * @return
         */
        private Optional<BindingSet> buildPropertyBindingSet(final RyaStatement statement) {
            final StatementMetadata metadata = statement.getMetadata();
            final Map<RyaIRI, RyaType> statementProps = metadata.getMetadata();
            if (statementProps.size() < properties.size()) {
                return Optional.empty();
            }
            final QueryBindingSet bs = new QueryBindingSet();
            for (final Map.Entry<RyaIRI, Var> entry : properties.entrySet()) {
                final RyaIRI key = entry.getKey();
                final Var var = entry.getValue();
                if (!statementProps.containsKey(key)) {
                    return Optional.empty();
                } else {
                    final Value val = RyaToRdfConversions.convertValue(statementProps.get(key));
                    if (var.getValue() == null) {
                        bs.addBinding(var.getName(), val);
                    } else if (!var.getValue().equals(val)) {
                        return Optional.empty();
                    }
                }
            }
            return Optional.of(bs);
        }

        /**
         * Builds the BindingSet from the specified RyaStatement by using the
         * StatementPattern for this class. This method checks whether
         * StatementPattern has a {@link Value} for each position
         * {@link org.eclipse.rdf4j.query.algebra.Var} (Subject, Predicate, Object).
         * If it doesn't have a Value, a Binding is created from the
         * RyaStatement using the {@link RyaType} for the corresponding position
         * (Subject, Predicate, Object).
         *
         * @param statement
         * @return BindingSet
         */
        private BindingSet buildBindingSetFromStatementPattern(final RyaStatement statement) {
            final Var subjVar = sp.getSubjectVar();
            final Var predVar = sp.getPredicateVar();
            final Var objVar = sp.getObjectVar();
            final Var contextVar = sp.getContextVar();
            final QueryBindingSet bs = new QueryBindingSet();

            if (subjVar.getValue() == null) {
                bs.addBinding(subjVar.getName(), RyaToRdfConversions.convertValue(statement.getSubject()));
            }

            if (predVar.getValue() == null) {
                bs.addBinding(predVar.getName(), RyaToRdfConversions.convertValue(statement.getPredicate()));
            }

            if (objVar.getValue() == null) {
                bs.addBinding(objVar.getName(), RyaToRdfConversions.convertValue(statement.getObject()));
            }

            if (contextVar != null && contextVar.getValue() == null) {
                bs.addBinding(contextVar.getName(), RyaToRdfConversions.convertValue(statement.getContext()));
            }

            return bs;
        }

        private boolean canJoinBindingSets(final BindingSet bs1, final BindingSet bs2) {
            for (final Binding b : bs1) {
                final String name = b.getName();
                final Value val = b.getValue();
                if (bs2.hasBinding(name) && (!bs2.getValue(name).equals(val))) {
                    return false;
                }
            }
            return true;
        }
    }

}
