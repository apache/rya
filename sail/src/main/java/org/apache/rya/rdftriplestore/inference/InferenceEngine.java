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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.persist.utils.RyaDaoQueryWrapper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.collect.Sets;

import info.aduna.iteration.CloseableIteration;

/**
 * Will pull down inference relationships from dao every x seconds. <br>
 * Will infer extra relationships. <br>
 * Will cache relationships in Graph for later use. <br>
 */
public class InferenceEngine {
    private static final Logger log = Logger.getLogger(InferenceEngine.class);
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final URI HAS_SELF = VF.createURI(OWL.NAMESPACE, "hasSelf");

    private Graph subClassOfGraph;
    private Graph subPropertyOfGraph;
    private Set<URI> symmetricPropertySet;
    private Map<URI, URI> inverseOfMap;
    private Set<URI> transitivePropertySet;
    private Map<URI, Set<URI>> domainByType;
    private Map<URI, Set<URI>> rangeByType;
    private Map<Resource, Map<URI, Value>> hasValueByType;
    private Map<URI, Map<Resource, Value>> hasValueByProperty;
    private Map<Resource, Map<Resource, URI>> allValuesFromByValueType;
    private final ConcurrentHashMap<Resource, List<Set<Resource>>> intersections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Resource, Set<Resource>> enumerations = new ConcurrentHashMap<>();
    // hasSelf maps.
    private Map<URI, Set<Resource>> hasSelfByProperty;
    private Map<Resource, Set<URI>> hasSelfByType;

    private RyaDAO<?> ryaDAO;
    private RdfCloudTripleStoreConfiguration conf;
    private RyaDaoQueryWrapper ryaDaoQueryWrapper;
    private boolean initialized = false;
    private boolean schedule = true;

    private long refreshGraphSchedule = 5 * 60 * 1000; //5 min
    private Timer timer;
    private HashMap<URI, List<URI>> propertyChainPropertyToChain = new HashMap<>();
    public static final String URI_PROP = "uri";

    public void init() throws InferenceEngineException {
        try {
            if (isInitialized()) {
                return;
            }

            checkNotNull(conf, "Configuration is null");
            checkNotNull(ryaDAO, "RdfDao is null");
            checkArgument(ryaDAO.isInitialized(), "RdfDao is not initialized");
            ryaDaoQueryWrapper = new RyaDaoQueryWrapper(ryaDAO, conf);

            if (schedule) {
                refreshGraph();
                timer = new Timer(InferenceEngine.class.getName());
                timer.scheduleAtFixedRate(new TimerTask() {

                    @Override
                    public void run() {
                        try {
                            refreshGraph();
                        } catch (final InferenceEngineException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }, refreshGraphSchedule, refreshGraphSchedule);
            }
            refreshGraph();
            setInitialized(true);
        } catch (final RyaDAOException e) {
            throw new InferenceEngineException(e);
        }
    }

    public void destroy() throws InferenceEngineException {
        setInitialized(false);
        if (timer != null) {
            timer.cancel();
        }
    }

    public void refreshGraph() throws InferenceEngineException {
        try {
            CloseableIteration<Statement, QueryEvaluationException> iter;
            //get all subclassof
            Graph graph = TinkerGraph.open();
            addPredicateEdges(RDFS.SUBCLASSOF, Direction.OUT, graph, RDFS.SUBCLASSOF.stringValue());
            //equivalentClass is the same as subClassOf both ways
            addPredicateEdges(OWL.EQUIVALENTCLASS, Direction.BOTH, graph, RDFS.SUBCLASSOF.stringValue());
            // Add unions to the subclass graph: if c owl:unionOf LIST(c1, c2, ... cn), then any
            // instances of c1, c2, ... or cn are also instances of c, meaning c is a superclass
            // of all the rest.
            // (In principle, an instance of c is likewise implied to be at least one of the other
            // types, but this fact is ignored for now to avoid nondeterministic reasoning.)
            iter = RyaDAOHelper.query(ryaDAO, null, OWL.UNIONOF, null, conf);
            try {
                while (iter.hasNext()) {
                    final Statement st = iter.next();
                    final Value unionType = st.getSubject();
                    // Traverse the list of types constituting the union
                    Value current = st.getObject();
                    while (current instanceof Resource && !RDF.NIL.equals(current)) {
                        final Resource listNode = (Resource) current;
                        CloseableIteration<Statement, QueryEvaluationException> listIter = RyaDAOHelper.query(ryaDAO,
                                listNode, RDF.FIRST, null, conf);
                        try {
                            if (listIter.hasNext()) {
                                final Statement firstStatement = listIter.next();
                                if (firstStatement.getObject() instanceof Resource) {
                                    final Resource subclass = (Resource) firstStatement.getObject();
                                    final Statement subclassStatement = VF.createStatement(subclass, RDFS.SUBCLASSOF, unionType);
                                    addStatementEdge(graph, RDFS.SUBCLASSOF.stringValue(), subclassStatement);
                                }
                            }
                        } finally {
                            listIter.close();
                        }
                        listIter = RyaDAOHelper.query(ryaDAO, listNode, RDF.REST, null, conf);
                        try {
                            if (listIter.hasNext()) {
                                current = listIter.next().getObject();
                            }
                            else {
                                current = RDF.NIL;
                            }
                        } finally {
                            listIter.close();
                        }
                    }
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            subClassOfGraph = graph; //TODO: Should this be synchronized?

            graph = TinkerGraph.open();
            addPredicateEdges(RDFS.SUBPROPERTYOF, Direction.OUT, graph, RDFS.SUBPROPERTYOF.stringValue());
            //equiv property really is the same as a subPropertyOf both ways
            addPredicateEdges(OWL.EQUIVALENTPROPERTY, Direction.BOTH, graph, RDFS.SUBPROPERTYOF.stringValue());
            subPropertyOfGraph = graph; //TODO: Should this be synchronized?

            refreshIntersectionOf();

            refreshOneOf();

            iter = RyaDAOHelper.query(ryaDAO, null, RDF.TYPE, OWL.SYMMETRICPROPERTY, conf);
            final Set<URI> symProp = new HashSet<>();
            try {
                while (iter.hasNext()) {
                    final Statement st = iter.next();
                    symProp.add((URI) st.getSubject()); //safe to assume it is a URI?
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            symmetricPropertySet = symProp;

            iter = RyaDAOHelper.query(ryaDAO, null, RDF.TYPE, OWL.TRANSITIVEPROPERTY, conf);
            final Set<URI> transProp = new HashSet<>();
            try {
                while (iter.hasNext()) {
                    final Statement st = iter.next();
                    transProp.add((URI) st.getSubject());
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            transitivePropertySet = transProp;

            iter = RyaDAOHelper.query(ryaDAO, null, OWL.INVERSEOF, null, conf);
            final Map<URI, URI> invProp = new HashMap<>();
            try {
                while (iter.hasNext()) {
                    final Statement st = iter.next();
                    invProp.put((URI) st.getSubject(), (URI) st.getObject());
                    invProp.put((URI) st.getObject(), (URI) st.getSubject());
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            inverseOfMap = invProp;

            iter = RyaDAOHelper.query(ryaDAO, null,
                    VF.createURI("http://www.w3.org/2002/07/owl#propertyChainAxiom"),
                    null, conf);
            final Map<URI,URI> propertyChainPropertiesToBNodes = new HashMap<>();
            propertyChainPropertyToChain = new HashMap<>();
            try {
                while (iter.hasNext()){
                    final Statement st = iter.next();
                    propertyChainPropertiesToBNodes.put((URI)st.getSubject(), (URI)st.getObject());
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            // now for each property chain bNode, get the indexed list of properties associated with that chain
            for (final URI propertyChainProperty : propertyChainPropertiesToBNodes.keySet()){
                final URI bNode = propertyChainPropertiesToBNodes.get(propertyChainProperty);
                // query for the list of indexed properties
                iter = RyaDAOHelper.query(ryaDAO, bNode, VF.createURI("http://www.w3.org/2000/10/swap/list#index"),
                        null, conf);
                final TreeMap<Integer, URI> orderedProperties = new TreeMap<>();
                // TODO refactor this.  Wish I could execute sparql
                try {
                    while (iter.hasNext()){
                        final Statement st = iter.next();
                        final String indexedElement = st.getObject().stringValue();
                        log.info(indexedElement);
                        CloseableIteration<Statement, QueryEvaluationException>  iter2 = RyaDAOHelper.query(ryaDAO, VF.createURI(st.getObject().stringValue()), RDF.FIRST,
                                null, conf);
                        String integerValue = "";
                        Value anonPropNode = null;
                        Value propURI = null;
                        if (iter2 != null){
                            while (iter2.hasNext()){
                                final Statement iter2Statement = iter2.next();
                                integerValue = iter2Statement.getObject().stringValue();
                                break;
                            }
                            iter2.close();
                        }
                        iter2 = RyaDAOHelper.query(ryaDAO, VF.createURI(st.getObject().stringValue()), RDF.REST,
                                null, conf);
                        if (iter2 != null){
                            while (iter2.hasNext()){
                                final Statement iter2Statement = iter2.next();
                                anonPropNode = iter2Statement.getObject();
                                break;
                            }
                            iter2.close();
                            if (anonPropNode != null){
                                iter2 = RyaDAOHelper.query(ryaDAO, VF.createURI(anonPropNode.stringValue()), RDF.FIRST,
                                        null, conf);
                                while (iter2.hasNext()){
                                    final Statement iter2Statement = iter2.next();
                                    propURI = iter2Statement.getObject();
                                    break;
                                }
                                iter2.close();
                            }
                        }
                        if (!integerValue.isEmpty() && propURI!=null) {
                            try {
                                final int indexValue = Integer.parseInt(integerValue);
                                final URI chainPropURI = VF.createURI(propURI.stringValue());
                                orderedProperties.put(indexValue, chainPropURI);
                            }
                            catch (final Exception ex){
                                // TODO log an error here

                            }
                        }
                    }
                } finally{
                    if (iter != null){
                        iter.close();
                    }
                }
                final List<URI> properties = new ArrayList<>();
                for (final Map.Entry<Integer, URI> entry : orderedProperties.entrySet()){
                    properties.add(entry.getValue());
                }
                propertyChainPropertyToChain.put(propertyChainProperty, properties);
            }

            // could also be represented as a list of properties (some of which may be blank nodes)
            for (final URI propertyChainProperty : propertyChainPropertiesToBNodes.keySet()){
                final List<URI> existingChain = propertyChainPropertyToChain.get(propertyChainProperty);
                // if we didn't get a chain, try to get it through following the collection
                if ((existingChain == null) || existingChain.isEmpty()) {

                    CloseableIteration<Statement, QueryEvaluationException>  iter2 = RyaDAOHelper.query(ryaDAO, propertyChainPropertiesToBNodes.get(propertyChainProperty), RDF.FIRST,
                            null, conf);
                    final List<URI> properties = new ArrayList<>();
                    URI previousBNode = propertyChainPropertiesToBNodes.get(propertyChainProperty);
                    if (iter2.hasNext()) {
                        Statement iter2Statement = iter2.next();
                        Value currentPropValue = iter2Statement.getObject();
                        while ((currentPropValue != null) && (!currentPropValue.stringValue().equalsIgnoreCase(RDF.NIL.stringValue()))){
                            if (currentPropValue instanceof URI){
                                iter2 = RyaDAOHelper.query(ryaDAO, VF.createURI(currentPropValue.stringValue()), RDF.FIRST,
                                        null, conf);
                                if (iter2.hasNext()){
                                    iter2Statement = iter2.next();
                                    if (iter2Statement.getObject() instanceof URI){
                                        properties.add((URI)iter2Statement.getObject());
                                    }
                                }
                                // otherwise see if there is an inverse declaration
                                else {
                                    iter2 = RyaDAOHelper.query(ryaDAO, VF.createURI(currentPropValue.stringValue()), OWL.INVERSEOF,
                                            null, conf);
                                    if (iter2.hasNext()){
                                        iter2Statement = iter2.next();
                                        if (iter2Statement.getObject() instanceof URI){
                                            properties.add(new InverseURI((URI)iter2Statement.getObject()));
                                        }
                                    }
                                }
                                // get the next prop pointer
                                iter2 = RyaDAOHelper.query(ryaDAO, previousBNode, RDF.REST,
                                        null, conf);
                                if (iter2.hasNext()){
                                    iter2Statement = iter2.next();
                                    previousBNode = (URI)currentPropValue;
                                    currentPropValue = iter2Statement.getObject();
                                }
                                else {
                                    currentPropValue = null;
                                }
                            }
                            else {
                                currentPropValue = null;
                            }

                        }
                        propertyChainPropertyToChain.put(propertyChainProperty, properties);
                    }
                }
            }

            refreshDomainRange();

            refreshPropertyRestrictions();

        } catch (final QueryEvaluationException e) {
            throw new InferenceEngineException(e);
        }
    }

    /**
     * Query for all triples involving a given predicate and add corresponding edges to a
     * {@link Graph} in one or both directions.
     * @param predicate Find all connections via this predicate URI
     * @param dir Direction of interest: for a matching triple, if {@link Direction#OUT} add an edge
     *      from subject to object; if {@link Direction#IN} add an edge from object to subject;
     *      if {@link Direction#BOTH} add both.
     * @param graph A TinkerPop graph
     * @param edgeName Label that will be given to all added edges
     * @throws QueryEvaluationException
     */
    private void addPredicateEdges(final URI predicate, final Direction dir, final Graph graph, final String edgeName)
            throws QueryEvaluationException {
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO,
                null, predicate, null, conf);
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                if (Direction.OUT.equals(dir) || Direction.BOTH.equals(dir)) {
                    addStatementEdge(graph, edgeName, st);
                }
                if (Direction.IN.equals(dir) || Direction.BOTH.equals(dir)) {
                    addStatementEdge(graph, edgeName, new StatementImpl((Resource) st.getObject(),
                            st.getPredicate(), st.getSubject()));
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Queries domain and range information, then populates the inference engine with direct
     * domain/range relations and any that can be inferred from the subclass graph, subproperty
     * graph, and inverse property map. Should be called after that class and property information
     * has been refreshed.
     *
     * Computes indirect domain/range:
     *  - If p1 has domain c, and p2 is a subproperty of p1, then p2 also has domain c.
     *  - If p1 has range c, and p2 is a subproperty of p1, then p2 also has range c.
     *  - If p1 has domain c, and p2 is the inverse of p1, then p2 has range c.
     *  - If p1 has range c, and p2 is the inverse of p1, then p2 has domain c.
     *  - If p has domain c1, and c1 is a subclass of c2, then p also has domain c2.
     *  - If p has range c1, and c1 is a subclass of c2, then p also has range c2.
     * @throws QueryEvaluationException
     */
    private void refreshDomainRange() throws QueryEvaluationException {
        final Map<URI, Set<URI>> domainByTypePartial = new ConcurrentHashMap<>();
        final Map<URI, Set<URI>> rangeByTypePartial = new ConcurrentHashMap<>();
        // First, populate domain and range based on direct domain/range triples.
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, RDFS.DOMAIN, null, conf);
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                final Resource property = st.getSubject();
                final Value domainType = st.getObject();
                if (domainType instanceof URI && property instanceof URI) {
                    if (!domainByTypePartial.containsKey(domainType)) {
                        domainByTypePartial.put((URI) domainType, new HashSet<>());
                    }
                    domainByTypePartial.get(domainType).add((URI) property);
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
        iter = RyaDAOHelper.query(ryaDAO, null, RDFS.RANGE, null, conf);
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                final Resource property = st.getSubject();
                final Value rangeType = st.getObject();
                if (rangeType instanceof URI && property instanceof URI) {
                    if (!rangeByTypePartial.containsKey(rangeType)) {
                        rangeByTypePartial.put((URI) rangeType, new HashSet<>());
                    }
                    rangeByTypePartial.get(rangeType).add((URI) property);
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
        // Then combine with the subclass/subproperty graphs and the inverse property map to compute
        // the closure of domain and range per class.
        final Set<URI> domainRangeTypeSet = new HashSet<>(domainByTypePartial.keySet());
        domainRangeTypeSet.addAll(rangeByTypePartial.keySet());
        // Extend to subproperties: make sure that using a more specific form of a property
        // still triggers its domain/range inferences.
        // Mirror for inverse properties: make sure that using the inverse form of a property
        // triggers the inverse domain/range inferences.
        // These two rules can recursively trigger one another.
        for (final URI domainRangeType : domainRangeTypeSet) {
            final Set<URI> propertiesWithDomain = domainByTypePartial.getOrDefault(domainRangeType, new HashSet<>());
            final Set<URI> propertiesWithRange = rangeByTypePartial.getOrDefault(domainRangeType, new HashSet<>());
            // Since findParents will traverse the subproperty graph and find all indirect
            // subproperties, the subproperty rule does not need to trigger itself directly.
            // And since no more than one inverseOf relationship is stored for any property, the
            // inverse property rule does not need to trigger itself directly. However, each rule
            // can trigger the other, so keep track of how the inferred domains/ranges were
            // discovered so we can apply only those rules that might yield new information.
            final Stack<URI> domainViaSuperProperty  = new Stack<>();
            final Stack<URI> rangeViaSuperProperty  = new Stack<>();
            final Stack<URI> domainViaInverseProperty  = new Stack<>();
            final Stack<URI> rangeViaInverseProperty  = new Stack<>();
            // Start with the direct domain/range assertions, which can trigger any rule.
            domainViaSuperProperty.addAll(propertiesWithDomain);
            domainViaInverseProperty.addAll(propertiesWithDomain);
            rangeViaSuperProperty.addAll(propertiesWithRange);
            rangeViaInverseProperty.addAll(propertiesWithRange);
            // Repeatedly infer domain/range from subproperties/inverse properties until no new
            // information can be generated.
            while (!(domainViaSuperProperty.isEmpty() && rangeViaSuperProperty.isEmpty()
                    && domainViaInverseProperty.isEmpty() && rangeViaInverseProperty.isEmpty())) {
                // For a type c and property p, if c is a domain of p, then c is the range of any
                // inverse of p. Would be redundant for properties discovered via inverseOf.
                while (!domainViaSuperProperty.isEmpty()) {
                    final URI property = domainViaSuperProperty.pop();
                    final URI inverseProperty = findInverseOf(property);
                    if (inverseProperty != null && propertiesWithRange.add(inverseProperty)) {
                        rangeViaInverseProperty.push(inverseProperty);
                    }
                }
                // For a type c and property p, if c is a range of p, then c is the domain of any
                // inverse of p. Would be redundant for properties discovered via inverseOf.
                while (!rangeViaSuperProperty.isEmpty()) {
                    final URI property = rangeViaSuperProperty.pop();
                    final URI inverseProperty = findInverseOf(property);
                    if (inverseProperty != null && propertiesWithDomain.add(inverseProperty)) {
                        domainViaInverseProperty.push(inverseProperty);
                    }
                }
                // For a type c and property p, if c is a domain of p, then c is also a domain of
                // p's subproperties. Would be redundant for properties discovered via this rule.
                while (!domainViaInverseProperty.isEmpty()) {
                    final URI property = domainViaInverseProperty.pop();
                    final Set<URI> subProperties = findParents(subPropertyOfGraph, property);
                    subProperties.removeAll(propertiesWithDomain);
                    propertiesWithDomain.addAll(subProperties);
                    domainViaSuperProperty.addAll(subProperties);
                }
                // For a type c and property p, if c is a range of p, then c is also a range of
                // p's subproperties. Would be redundant for properties discovered via this rule.
                while (!rangeViaInverseProperty.isEmpty()) {
                    final URI property = rangeViaInverseProperty.pop();
                    final Set<URI> subProperties = findParents(subPropertyOfGraph, property);
                    subProperties.removeAll(propertiesWithRange);
                    propertiesWithRange.addAll(subProperties);
                    rangeViaSuperProperty.addAll(subProperties);
                }
            }
            if (!propertiesWithDomain.isEmpty()) {
                domainByTypePartial.put(domainRangeType, propertiesWithDomain);
            }
            if (!propertiesWithRange.isEmpty()) {
                rangeByTypePartial.put(domainRangeType, propertiesWithRange);
            }
        }
        // Once all properties have been found for each domain/range class, extend to superclasses:
        // make sure that the consequent of a domain/range inference goes on to apply any more
        // general classes as well.
        for (final URI subtype : domainRangeTypeSet) {
            final Set<URI> supertypes = getSuperClasses(subtype);
            final Set<URI> propertiesWithDomain = domainByTypePartial.getOrDefault(subtype, new HashSet<>());
            final Set<URI> propertiesWithRange = rangeByTypePartial.getOrDefault(subtype, new HashSet<>());
            for (final URI supertype : supertypes) {
                // For a property p and its domain c: all of c's superclasses are also domains of p.
                if (!propertiesWithDomain.isEmpty() && !domainByTypePartial.containsKey(supertype)) {
                    domainByTypePartial.put(supertype, new HashSet<>());
                }
                for (final URI property : propertiesWithDomain) {
                    domainByTypePartial.get(supertype).add(property);
                }
                // For a property p and its range c: all of c's superclasses are also ranges of p.
                if (!propertiesWithRange.isEmpty() && !rangeByTypePartial.containsKey(supertype)) {
                    rangeByTypePartial.put(supertype, new HashSet<>());
                }
                for (final URI property : propertiesWithRange) {
                    rangeByTypePartial.get(supertype).add(property);
                }
            }
        }
        domainByType = domainByTypePartial;
        rangeByType = rangeByTypePartial;
    }

    private void refreshPropertyRestrictions() throws QueryEvaluationException {
        // Get a set of all property restrictions of any type
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.ONPROPERTY, null, conf);
        final Map<Resource, URI> restrictions = new HashMap<>();
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                restrictions.put(st.getSubject(), (URI) st.getObject());
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
        // Query for specific types of restriction and add their details to the schema
        refreshHasValueRestrictions(restrictions);
        refreshAllValuesFromRestrictions(restrictions);
        refreshHasSelfRestrictions(restrictions);
    }

    private void refreshHasValueRestrictions(final Map<Resource, URI> restrictions) throws QueryEvaluationException {
        hasValueByType = new HashMap<>();
        hasValueByProperty = new HashMap<>();
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.HASVALUE, null, conf);
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                final Resource restrictionClass = st.getSubject();
                if (restrictions.containsKey(restrictionClass)) {
                    final URI property = restrictions.get(restrictionClass);
                    final Value value = st.getObject();
                    if (!hasValueByType.containsKey(restrictionClass)) {
                        hasValueByType.put(restrictionClass, new HashMap<>());
                    }
                    if (!hasValueByProperty.containsKey(property)) {
                        hasValueByProperty.put(property, new HashMap<>());
                    }
                    hasValueByType.get(restrictionClass).put(property, value);
                    hasValueByProperty.get(property).put(restrictionClass, value);
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    private void refreshAllValuesFromRestrictions(final Map<Resource, URI> restrictions) throws QueryEvaluationException {
        allValuesFromByValueType = new HashMap<>();
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.ALLVALUESFROM, null, conf);
        try {
            while (iter.hasNext()) {
                final Statement st = iter.next();
                if (restrictions.containsKey(st.getSubject()) && st.getObject() instanceof URI) {
                    final URI property = restrictions.get(st.getSubject());
                    final URI valueClass = (URI) st.getObject();
                    // Should also be triggered by subclasses of the property restriction
                    final Set<Resource> restrictionClasses = new HashSet<>();
                    restrictionClasses.add(st.getSubject());
                    if (st.getSubject() instanceof URI) {
                        restrictionClasses.addAll(getSubClasses((URI) st.getSubject()));
                    }
                    for (final Resource restrictionClass : restrictionClasses) {
                        if (!allValuesFromByValueType.containsKey(valueClass)) {
                            allValuesFromByValueType.put(valueClass, new HashMap<>());
                        }
                        allValuesFromByValueType.get(valueClass).put(restrictionClass, property);
                    }
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    private void refreshHasSelfRestrictions(final Map<Resource, URI> restrictions) throws QueryEvaluationException {
        hasSelfByType = new HashMap<>();
        hasSelfByProperty = new HashMap<>();

        for(final Resource type : restrictions.keySet()) {
            final URI property = restrictions.get(type);
            final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, type, HAS_SELF, null, conf);
            try {
                if (iter.hasNext()) {
                    Set<URI> typeSet = hasSelfByType.get(type);
                    Set<Resource> propSet = hasSelfByProperty.get(property);

                    if (typeSet == null) {
                        typeSet = new HashSet<>();
                    }
                    if (propSet == null) {
                        propSet = new HashSet<>();
                    }

                    typeSet.add(property);
                    propSet.add(type);

                    hasSelfByType.put(type, typeSet);
                    hasSelfByProperty.put(property, propSet);
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
        }
    }

    private void refreshIntersectionOf() throws QueryEvaluationException {
        final Map<Resource, List<Set<Resource>>> intersectionsProp = new HashMap<>();

        // First query for all the owl:intersectionOf's.
        // If we have the following intersectionOf:
        // :A owl:intersectionOf[:B, :C]
        // It will be represented by triples following a pattern similar to:
        // <:A> owl:intersectionOf _:bnode1 .
        //  _:bnode1 rdf:first <:B> .
        //  _:bnode1 rdf:rest _:bnode2 .
        // _:bnode2 rdf:first <:C> .
        // _:bnode2 rdf:rest rdf:nil .
        ryaDaoQueryWrapper.queryAll(null, OWL.INTERSECTIONOF, null, new RDFHandlerBase() {
            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                final Resource type = statement.getSubject();
                // head will point to a type that is part of the intersection.
                final URI head = (URI) statement.getObject();
                if (!intersectionsProp.containsKey(type)) {
                    intersectionsProp.put(type, new ArrayList<Set<Resource>>());
                }

                // head should point to a list of items that forms the
                // intersection.
                try {
                    final Set<Resource> intersection = new LinkedHashSet<>(getList(head));
                    if (!intersection.isEmpty()) {
                        // Add this intersection for this type. There may be more
                        // intersections for this type so each type has a list of
                        // intersection sets.
                        intersectionsProp.get(type).add(intersection);
                    }
                } catch (final QueryEvaluationException e) {
                    throw new RDFHandlerException("Error getting intersection list.", e);
                }
            }
        });

        intersections.clear();
        for (final Entry<Resource, List<Set<Resource>>> entry : intersectionsProp.entrySet()) {
            final Resource type = entry.getKey();
            final List<Set<Resource>> intersectionList = entry.getValue();
            final Set<Resource> otherTypes = new HashSet<>();
            // Combine all of a type's intersections together.
            for (final Set<Resource> intersection : intersectionList) {
                otherTypes.addAll(intersection);
            }
            for (final Resource other : otherTypes) {
                // :A intersectionOf[:B, :C] implies that
                // :A subclassOf :B
                // :A subclassOf :C
                // So add each type that's part of the intersection to the
                // subClassOf graph.
                addSubClassOf(type, other);
                for (final Set<Resource> intersection : intersectionList) {
                    if (!intersection.contains(other)) {
                        addIntersection(intersection, other);
                    }
                }
            }
            for (final Set<Resource> intersection : intersectionList) {
                addIntersection(intersection, type);
            }
        }
        for (final Entry<Resource, List<Set<Resource>>> entry : intersectionsProp.entrySet()) {
            final Resource type = entry.getKey();
            final List<Set<Resource>> intersectionList = entry.getValue();

            final Set<URI> superClasses = getSuperClasses((URI) type);
            for (final URI superClass : superClasses) {
                // Add intersections to super classes if applicable.
                // IF:
                // :A intersectionOf[:B, :C]
                // AND
                // :A subclassOf :D
                // Then we can infer:
                // intersectionOf[:B, :C] subclassOf :D
                for (final Set<Resource> intersection : intersectionList) {
                    addIntersection(intersection, superClass);
                }
            }
            // Check if other keys have any of the same intersections and infer
            // the same subclass logic to them that we know from the current
            // type. Propagating up through all the superclasses.
            for (final Set<Resource> intersection : intersectionList) {
                final Set<Resource> otherKeys = Sets.newHashSet(intersectionsProp.keySet());
                otherKeys.remove(type);
                for (final Resource otherKey : otherKeys) {
                    if (intersectionsProp.get(otherKey).contains(intersection)) {
                        addSubClassOf(otherKey, type);
                        addSubClassOf(type, otherKey);
                    }
                }
            }
        }
    }

    private void refreshOneOf() throws QueryEvaluationException {
        final Map<Resource, Set<Resource>> enumTypes = new HashMap<>();

        // First query for all the owl:oneOf's.
        // If we have the following oneOf:
        // :A owl:oneOf (:B, :C)
        // It will be represented by triples following a pattern similar to:
        // <:A> owl:oneOf _:bnode1 .
        //  _:bnode1 rdf:first <:B> .
        //  _:bnode1 rdf:rest _:bnode2 .
        // _:bnode2 rdf:first <:C> .
        // _:bnode2 rdf:rest rdf:nil .
        ryaDaoQueryWrapper.queryAll(null, OWL.ONEOF, null, new RDFHandlerBase() {
            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                final Resource enumType = statement.getSubject();
                // listHead will point to a type class of the enumeration.
                final URI listHead = (URI) statement.getObject();
                if (!enumTypes.containsKey(enumType)) {
                    enumTypes.put(enumType, new LinkedHashSet<Resource>());
                }

                // listHead should point to a list of items that forms the
                // enumeration.
                try {
                    final Set<Resource> enumeration = new LinkedHashSet<>(getList(listHead));
                    if (!enumeration.isEmpty()) {
                        // Add this enumeration for this type.
                        enumTypes.get(enumType).addAll(enumeration);
                    }
                } catch (final QueryEvaluationException e) {
                    throw new RDFHandlerException("Error getting enumeration list.", e);
                }
            }
        });

        enumerations.clear();
        enumerations.putAll(enumTypes);
    }

    /**
     * For a given type, return any properties such that some owl:hasSelf
     * restrictions implies that properties of this type have the value of
     * themselves for this type.
     *
     * This takes into account type hierarchy, where children of a type that
     * have this property are also assumed to have the property.
     * 
     * @param type
     *            The type (URI or bnode) to check against the known
     *            restrictions
     * @return For each relevant property, a set of values such that whenever a
     *         resource has that value for that property, it is implied to
     *         belong to the type.
     */
    public Set<URI> getHasSelfImplyingType(final Resource type){
        // return properties that imply this type if reflexive
        final Set<URI> properties = new HashSet<>();
        Set<URI> tempProperties = hasSelfByType.get(type);

        if (tempProperties != null) {
            properties.addAll(tempProperties);
        }
        //findParent gets all subclasses, add self.
        if (type instanceof URI) {
            for (final URI subtype : findParents(subClassOfGraph, (URI) type)) {
                tempProperties = hasSelfByType.get(subtype);
                if (tempProperties != null) {
                    properties.addAll(tempProperties);
                }
            }
        }

        // make map hasSelfByType[]
        return properties;
    }

    /**
     * For a given property, return any types such that some owl:hasSelf restriction implies that members
     * of the type have the value of themselves for this property.
     *
     * This takes into account type hierarchy, where children of a type that have
     * this property are also assumed to have the property.
     * @param property The property whose owl:hasSelf restrictions to return
     * @return A set of types that possess the implied property.
     */
    public Set<Resource> getHasSelfImplyingProperty(final URI property) {
        // return types that imply this type if reflexive
        final Set<Resource> types = new HashSet<>();
        final Set<Resource> baseTypes = hasSelfByProperty.get(property);

        if (baseTypes != null) {
            types.addAll(baseTypes);

            // findParent gets all subclasses, add self.
            for (final Resource baseType : baseTypes) {
                if (baseType instanceof URI) {
                    types.addAll(findParents(subClassOfGraph, (URI) baseType));
                }
            }
        }

        // make map hasSelfByProperty[]
        return types;
    }

    /**
     * Queries for all items that are in a list of the form:
     * <pre>
     *     <:A> ?x _:bnode1 .
     *     _:bnode1 rdf:first <:B> .
     *     _:bnode1 rdf:rest _:bnode2 .
     *     _:bnode2 rdf:first <:C> .
     *     _:bnode2 rdf:rest rdf:nil .
     * </pre>
     * Where {@code :_bnode1} represents the first item in the list and
     * {@code ?x} is some restriction on {@code <:A>}. This will return the
     * list of resources, {@code [<:B>, <:C>]}.
     * @param firstItem the first item in the list.
     * @return the {@link List} of {@link Resource}s.
     * @throws QueryEvaluationException
     */
    private List<Resource> getList(final URI firstItem) throws QueryEvaluationException {
        URI head = firstItem;
        final List<Resource> list = new ArrayList<>();
        // Go through and find all bnodes that are part of the defined list.
        while (!RDF.NIL.equals(head)) {
            // rdf.first will point to a type item that is in the list.
            ryaDaoQueryWrapper.queryFirst(head, RDF.FIRST, null, new RDFHandlerBase() {
                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    // The object found in the query represents a type
                    // that should be included in the list.
                    final URI object = (URI) statement.getObject();
                    list.add(object);
                }
            });
            final MutableObject<URI> headHolder = new MutableObject<>();
            // rdf.rest will point to the next bnode that's part of the list.
            ryaDaoQueryWrapper.queryFirst(head, RDF.REST, null, new RDFHandlerBase() {
                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    // This object is the next bnode head to look for.
                    final URI object = (URI) statement.getObject();
                    headHolder.setValue(object);
                }
            });
            // As long as we get a new head there are more bnodes that are part
            // of the list. Keep going until we reach rdf.nil.
            if (headHolder.getValue() != null) {
                head = headHolder.getValue();
            } else {
                head = RDF.NIL;
            }
        }
        return list;
    }

    private void addSubClassOf(final Resource s, final Resource o) {
        final Statement statement = new StatementImpl(s, RDFS.SUBCLASSOF, o);
        final String edgeName = RDFS.SUBCLASSOF.stringValue();
        addStatementEdge(subClassOfGraph, edgeName, statement);
    }

    private void addIntersection(final Set<Resource> intersection, final Resource type) {
        if (type != null && intersection != null && !intersection.isEmpty()) {
            List<Set<Resource>> intersectionList = intersections.get(type);
            if (intersectionList == null) {
                intersectionList = new ArrayList<>();
            }
            if (!intersectionList.contains(intersection)) {
                intersectionList.add(intersection);
            }
            intersections.put(type, intersectionList);
        }
    }

    private static Vertex getVertex(final Graph graph, final Object id) {
        final Iterator<Vertex> it = graph.vertices(id.toString());
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    private static void addStatementEdge(final Graph graph, final String edgeName, final Statement st) {
        final Resource subj = st.getSubject();
        Vertex a = getVertex(graph, subj);
        if (a == null) {
            a = graph.addVertex(T.id, subj.toString());
            a.property(URI_PROP, subj);
        }
        final Value obj = st.getObject();
        Vertex b = getVertex(graph, obj);
        if (b == null) {
            b = graph.addVertex(T.id, obj.toString());
            b.property(URI_PROP, obj);
        }
        a.addEdge(edgeName, b);
    }

    /**
     * Returns all super class types of the specified type based on the
     * internal subclass graph.
     * @param type the type {@link URI} to find super classes for.
     * @return the {@link Set} of {@link URI} types that are super classes types
     * of the specified {@code type}. Returns an empty set if nothing was found.
     */
    public Set<URI> getSuperClasses(final URI type) {
        return findChildren(subClassOfGraph, type);
    }

    /**
     * Returns all sub class types of the specified type based on the
     * internal subclass graph.
     * @param type the type {@link URI} to find sub classes for.
     * @return the {@link Set} of {@link URI} types that are sub classes types
     * of the specified {@code type}. Returns an empty set if nothing was found.
     */
    public Set<URI> getSubClasses(final URI type) {
        return findParents(subClassOfGraph, type);
    }

    public static Set<URI> findParents(final Graph graph, final URI vertexId) {
        return findParents(graph, vertexId, true);
    }

    public static Set<URI> findParents(final Graph graph, final URI vertexId, final boolean isRecursive) {
        return findConnected(graph, vertexId, Direction.IN, isRecursive);
    }

    public static Set<URI> findChildren(final Graph graph, final URI vertexId) {
        return findChildren(graph, vertexId, true);
    }

    public static Set<URI> findChildren(final Graph graph, final URI vertexId, final boolean isRecursive) {
        return findConnected(graph, vertexId, Direction.OUT, isRecursive);
    }

    private static Set<URI> findConnected(final Graph graph, final URI vertexId, final Direction traversal, final boolean isRecursive) {
        final Set<URI> connected = new HashSet<>();
        if (graph == null) {
            return connected;
        }
        final Vertex v = getVertex(graph, vertexId);
        if (v == null) {
            return connected;
        }
        addConnected(v, connected, traversal, isRecursive);
        return connected;
    }

    private static void addConnected(final Vertex v, final Set<URI> connected, final Direction traversal, final boolean isRecursive) {
        v.edges(traversal).forEachRemaining(edge -> {
            final Vertex ov = edge.vertices(traversal.opposite()).next();
            final Object o = ov.property(URI_PROP).value();
            if (o != null && o instanceof URI) {
                final boolean contains = connected.contains(o);
                if (!contains) {
                    connected.add((URI) o);
                    if (isRecursive) {
                        addConnected(ov, connected, traversal, isRecursive);
                    }
                }
            }
        });
    }

    public boolean isSymmetricProperty(final URI prop) {
        return (symmetricPropertySet != null) && symmetricPropertySet.contains(prop);
    }

    public URI findInverseOf(final URI prop) {
        return (inverseOfMap != null) ? inverseOfMap.get(prop) : (null);
    }

    public boolean isTransitiveProperty(final URI prop) {
        return (transitivePropertySet != null) && transitivePropertySet.contains(prop);
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public Set<Statement> findTransitiveProperty(final Resource subj, final URI prop, final Value obj, final Resource... contxts) throws InferenceEngineException {
        if (transitivePropertySet.contains(prop)) {
            final Set<Statement> sts = new HashSet<>();
            final boolean goUp = subj == null;
            chainTransitiveProperty(subj, prop, obj, (goUp) ? (obj) : (subj), sts, goUp, contxts);
            return sts;
        } else {
            return null;
        }
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public Set<Resource> findSameAs(final Resource value, final Resource... contxts) throws InferenceEngineException{
        final Set<Resource> sameAs = new HashSet<>();
        sameAs.add(value);
        findSameAsChaining(value, sameAs, contxts);
        return sameAs;
    }

    public CloseableIteration<Statement, QueryEvaluationException> queryDao(final Resource subject, final URI predicate, final Value object, final Resource... contexts) throws QueryEvaluationException {
        return RyaDAOHelper.query(ryaDAO, subject, predicate, object, conf, contexts);
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public void findSameAsChaining(final Resource subj, final Set<Resource> currentSameAs, final Resource[] contxts) throws InferenceEngineException{
        CloseableIteration<Statement, QueryEvaluationException> subjIter = null;
        CloseableIteration<Statement, QueryEvaluationException> objIter = null;
        try {
            subjIter = queryDao(subj, OWL.SAMEAS, null, contxts);
            while (subjIter.hasNext()){
                final Statement st = subjIter.next();
                if (!currentSameAs.contains(st.getObject())){
                    final Resource castedObj = (Resource) st.getObject();
                    currentSameAs.add(castedObj);
                    findSameAsChaining(castedObj, currentSameAs, contxts);
                }
            }
            objIter = queryDao(null, OWL.SAMEAS, subj, contxts);
            while (objIter.hasNext()){
                final Statement st = objIter.next();
                if (!currentSameAs.contains(st.getSubject())){
                    final Resource sameAsSubj = st.getSubject();
                    currentSameAs.add(sameAsSubj);
                    findSameAsChaining(sameAsSubj, currentSameAs, contxts);
                }
            }
        } catch (final QueryEvaluationException e) {
            throw new InferenceEngineException(e);
        } finally {
            if (subjIter != null) {
                try {
                    subjIter.close();
                } catch (final QueryEvaluationException e) {
                    throw new InferenceEngineException("Error while closing \"same as chaining\" statement subject iterator.", e);
                }
            }
            if (objIter != null) {
                try {
                    objIter.close();
                } catch (final QueryEvaluationException e) {
                    throw new InferenceEngineException("Error while closing \"same as chaining\" statement object iterator.", e);
                }
            }
        }
    }

    protected void chainTransitiveProperty(final Resource subj, final URI prop, final Value obj, final Value core, final Set<Statement> sts, final boolean goUp, final Resource[] contxts) throws InferenceEngineException {
        CloseableIteration<Statement, QueryEvaluationException> iter = null;
        try {
            iter = queryDao(subj, prop, obj, contxts);
            while (iter.hasNext()) {
                final Statement st = iter.next();
                sts.add(new StatementImpl((goUp) ? (st.getSubject()) : (Resource) (core), prop, (!goUp) ? (st.getObject()) : (core)));
                if (goUp) {
                    chainTransitiveProperty(null, prop, st.getSubject(), core, sts, goUp, contxts);
                } else {
                    chainTransitiveProperty((Resource) st.getObject(), prop, null, core, sts, goUp, contxts);
                }
            }
        } catch (final QueryEvaluationException e) {
            throw new InferenceEngineException(e);
        } finally {
            if (iter != null) {
                try {
                    iter.close();
                } catch (final QueryEvaluationException e) {
                    throw new InferenceEngineException("Error while closing \"chain transitive\" property statement iterator.", e);
                }
            }
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(final boolean initialized) {
        this.initialized = initialized;
    }

    public RyaDAO<?> getRyaDAO() {
        return ryaDAO;
    }

    public void setRyaDAO(final RyaDAO<?> ryaDAO) {
        this.ryaDAO = ryaDAO;
        ryaDaoQueryWrapper = new RyaDaoQueryWrapper(ryaDAO);
    }

    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }

    public void setConf(final RdfCloudTripleStoreConfiguration conf) {
        this.conf = conf;
    }

    public Graph getSubClassOfGraph() {
        return subClassOfGraph;
    }

    public Map<URI, List<URI>> getPropertyChainMap() {
        return propertyChainPropertyToChain;
    }

    public List<URI> getPropertyChain(final URI chainProp) {
        if (propertyChainPropertyToChain.containsKey(chainProp)){
            return propertyChainPropertyToChain.get(chainProp);
        }
        return new ArrayList<URI>();
    }

    public Graph getSubPropertyOfGraph() {
        return subPropertyOfGraph;
    }

    public long getRefreshGraphSchedule() {
        return refreshGraphSchedule;
    }

    public void setRefreshGraphSchedule(final long refreshGraphSchedule) {
        this.refreshGraphSchedule = refreshGraphSchedule;
    }

    public Set<URI> getSymmetricPropertySet() {
        return symmetricPropertySet;
    }

    public void setSymmetricPropertySet(final Set<URI> symmetricPropertySet) {
        this.symmetricPropertySet = symmetricPropertySet;
    }

    public Map<URI, URI> getInverseOfMap() {
        return inverseOfMap;
    }

    public void setInverseOfMap(final Map<URI, URI> inverseOfMap) {
        this.inverseOfMap = inverseOfMap;
    }

    public Set<URI> getTransitivePropertySet() {
        return transitivePropertySet;
    }

    public void setTransitivePropertySet(final Set<URI> transitivePropertySet) {
        this.transitivePropertySet = transitivePropertySet;
    }

    public boolean isSchedule() {
        return schedule;
    }

    public void setSchedule(final boolean schedule) {
        this.schedule = schedule;
    }

    /**
     * For a given type, return any properties and values such that owl:hasValue restrictions on
     * those properties could imply this type. No matter how many restrictions are returned, each
     * one is considered individually sufficient: if a resource has the property and the value, then
     * it belongs to the provided type. Takes type hierarchy into account, so the value may imply a
     * subtype which in turn implies the provided type.
     * @param type The type (URI or bnode) to check against the known restrictions
     * @return For each relevant property, a set of values such that whenever a resource has that
     *      value for that property, it is implied to belong to the type.
     */
    public Map<URI, Set<Value>> getHasValueByType(final Resource type) {
        final Map<URI, Set<Value>> implications = new HashMap<>();
        if (hasValueByType != null) {
            final Set<Resource> types = new HashSet<>();
            types.add(type);
            if (type instanceof URI) {
                types.addAll(getSubClasses((URI) type));
            }
            for (final Resource relevantType : types) {
                if (hasValueByType.containsKey(relevantType)) {
                    for (final Map.Entry<URI, Value> propertyToValue : hasValueByType.get(relevantType).entrySet()) {
                        if (!implications.containsKey(propertyToValue.getKey())) {
                            implications.put(propertyToValue.getKey(), new HashSet<>());
                        }
                        implications.get(propertyToValue.getKey()).add(propertyToValue.getValue());
                    }
                }
            }
        }
        return implications;
    }

    /**
     * For a given property, return any types and values such that some owl:hasValue restriction
     * states that members of the type are implied to have the associated specific value(s) for
     * this property. Takes class hierarchy into account, which means one type may imply multiple
     * values by way of supertypes with their own restrictions. Does not consider the property
     * hierarchy, so only restrictions that directly reference the given property (using
     * owl:onProperty) are relevant.
     * @param property The property whose owl:hasValue restrictions to return
     * @return A mapping from type (URIs or bnodes) to the set of any values that belonging to that
     *      type implies.
     */
    public Map<Resource, Set<Value>> getHasValueByProperty(final URI property) {
        final Map<Resource, Set<Value>> implications = new HashMap<>();
        if (hasValueByProperty != null && hasValueByProperty.containsKey(property)) {
            for (final Map.Entry<Resource, Value> typeToValue : hasValueByProperty.get(property).entrySet()) {
                final Resource type = typeToValue.getKey();
                if (!implications.containsKey(type)) {
                    implications.put(type, new HashSet<>());
                }
                implications.get(type).add(typeToValue.getValue());
                if (type instanceof URI) {
                    for (final URI subtype : getSubClasses((URI) type)) {
                        if (!implications.containsKey(subtype)) {
                            implications.put(subtype, new HashSet<>());
                        }
                        implications.get(subtype).add(typeToValue.getValue());
                    }
                }
            }
        }
        return implications;
    }

    /**
     * For a given type, get all properties which have that type as a domain. That type can be
     * inferred for any resource which is a subject of any triple involving one of these properties.
     * Accounts for class and property hierarchy, for example that a subproperty implicitly has its
     * superproperty's domain, as well as inverse properties, where a property's range is its
     * inverse property's domain.
     * @param domainType The type to check against the known domains
     * @return The set of properties with domain of that type, meaning that any triple whose
     *      predicate belongs to that set implies that the triple's subject belongs to the type.
     */
    public Set<URI> getPropertiesWithDomain(final URI domainType) {
        final Set<URI> properties = new HashSet<>();
        if (domainByType.containsKey(domainType)) {
            properties.addAll(domainByType.get(domainType));
        }
        return properties;
    }

    /**
     * For a given type, get all properties which have that type as a range. That type can be
     * inferred for any resource which is an object of any triple involving one of these properties.
     * Accounts for class and property hierarchy, for example that a subproperty implicitly has its
     * superproperty's range, as well as inverse properties, where a property's domain is its
     * inverse property's range.
     * @param rangeType The type to check against the known ranges
     * @return The set of properties with range of that type, meaning that any triple whose
     *      predicate belongs to that set implies that the triple's object belongs to the type.
     */
    public Set<URI> getPropertiesWithRange(final URI rangeType) {
        final Set<URI> properties = new HashSet<>();
        if (rangeByType.containsKey(rangeType)) {
            properties.addAll(rangeByType.get(rangeType));
        }
        return properties;
    }

    /**
     * For a given type, return information about any owl:allValuesFrom restriction that could imply
     * an individual's membership in that type: If the subject of a triple belongs to the type
     * associated with the restriction itself, and the predicate is the one referenced by the
     * restriction, then the object of the triple is implied to have the value type.
     * @param valueType The type to be inferred, which is the type of the object of the triple, or
     *      the type from which all values are stated to belong. Takes class hierarchy into account,
     *      so possible inferences include any ways of inferring subtypes of the value type, and
     *      subject types that trigger inference include any subtypes of relevant restrictions.
     *      Also considers property hierarchy, so properties that trigger inference will include
     *      subproperties of those referenced by relevant restrictions.
     * @return A map from subject type (a property restriction type or a subtype of one) to the set
     *      of properties (including any property referenced by such a restriction and all of its
     *      subproperties) such that for any individual which belongs to the subject type, all
     *      values it has for any of those properties belong to the value type.
     */
    public Map<Resource, Set<URI>> getAllValuesFromByValueType(final Resource valueType) {
        final Map<Resource, Set<URI>> implications = new HashMap<>();
        if (allValuesFromByValueType != null) {
            // Check for any subtypes which would in turn imply the value type
            final HashSet<Resource> valueTypes = new HashSet<>();
            valueTypes.add(valueType);
            if (valueType instanceof URI) {
                valueTypes.addAll(getSubClasses((URI) valueType));
            }
            for (final Resource valueSubType : valueTypes) {
                if (allValuesFromByValueType.containsKey(valueSubType)) {
                    final Map<Resource, URI> restrictionToProperty = allValuesFromByValueType.get(valueSubType);
                    for (final Resource restrictionType : restrictionToProperty.keySet()) {
                        if (!implications.containsKey(restrictionType)) {
                            implications.put(restrictionType, new HashSet<>());
                        }
                        final URI property = restrictionToProperty.get(restrictionType);
                        implications.get(restrictionType).add(property);
                        // Also add subproperties that would in turn imply the property
                        implications.get(restrictionType).addAll(findParents(subPropertyOfGraph, property));
                    }
                }
            }
        }
        return implications;
    }

    /**
     * For a given type, return all sets of types such that owl:intersectionOf
     * restrictions on those properties could imply this type.  A type can have
     * multiple intersections and each intersection is a set of types so a list
     * of all the type sets is returned.
     * @param type The type (URI or bnode) to check against the known
     * intersections.
     * @return A {@link List} of {@link Resource} type {@link Set}s that
     * represents all the known intersections that imply the specified type.
     * {@code null} is returned if no intersections were found for the specified
     * type.
     */
    public List<Set<Resource>> getIntersectionsImplying(final Resource type) {
        if (intersections != null) {
            final List<Set<Resource>> intersectionList = intersections.get(type);
            return intersectionList;
        }
        return null;
    }

    /**
     * For a given type, return all sets of types such that owl:oneOf
     * restrictions on those properties could imply this type. A enumeration
     * of all the types that are part of the specified class type.
     * @param type The type (URI or bnode) to check against the known oneOf
     * sets.
     * @return A {@link Set} of {@link Resource} types that represents the
     * enumeration of resources that belong to the class type.
     * An empty set is returned if no enumerations were found for the specified
     * type.
     */
    public Set<Resource> getEnumeration(final Resource type) {
        if (enumerations != null) {
            final Set<Resource> oneOfSet = enumerations.get(type);
            if (oneOfSet != null) {
                return oneOfSet;
            }
        }
        return new LinkedHashSet<>();

    }

    /**
     * Checks if the specified type is an enumerated type.
     * @param type The type (URI or bnode) to check against the known oneOf
     * sets.
     * @return {@code true} if the type is an enumerated type. {@code false}
     * otherwise.
     */
    public boolean isEnumeratedType(final Resource type) {
        return enumerations != null && enumerations.containsKey(type);
    }
}
