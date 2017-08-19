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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.collect.Iterators;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.utils.RyaDAOHelper;

/**
 * Will pull down inference relationships from dao every x seconds. <br>
 * Will infer extra relationships. <br>
 * Will cache relationships in Graph for later use. <br>
 */
public class InferenceEngine {

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

    private RyaDAO ryaDAO;
    private RdfCloudTripleStoreConfiguration conf;
    private boolean initialized = false;
    private boolean schedule = true;

    private long refreshGraphSchedule = 5 * 60 * 1000; //5 min
    private Timer timer;
	private HashMap<URI, List<URI>> propertyChainPropertyToChain = new HashMap<URI, List<URI>>();
    public static final String URI_PROP = "uri";

    public void init() throws InferenceEngineException {
        try {
            if (isInitialized()) {
                return;
            }

            checkNotNull(conf, "Configuration is null");
            checkNotNull(ryaDAO, "RdfDao is null");
            checkArgument(ryaDAO.isInitialized(), "RdfDao is not initialized");

            if (schedule) {
            	refreshGraph();
                timer = new Timer(InferenceEngine.class.getName());
                timer.scheduleAtFixedRate(new TimerTask() {

                    @Override
                    public void run() {
                        try {
                            refreshGraph();
                        } catch (InferenceEngineException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }, refreshGraphSchedule, refreshGraphSchedule);
            }
            refreshGraph();
            setInitialized(true);
        } catch (RyaDAOException e) {
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
        ValueFactory vf = ValueFactoryImpl.getInstance();
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
                    Statement st = iter.next();
                    Value unionType = st.getSubject();
                    // Traverse the list of types constituting the union
                    Value current = st.getObject();
                    while (current instanceof Resource && !RDF.NIL.equals(current)) {
                        Resource listNode = (Resource) current;
                        CloseableIteration<Statement, QueryEvaluationException> listIter = RyaDAOHelper.query(ryaDAO,
                                listNode, RDF.FIRST, null, conf);
                        try {
                            if (listIter.hasNext()) {
                                Statement firstStatement = listIter.next();
                                if (firstStatement.getObject() instanceof Resource) {
                                    Resource subclass = (Resource) firstStatement.getObject();
                                    Statement subclassStatement = vf.createStatement(subclass, RDFS.SUBCLASSOF, unionType);
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

            iter = RyaDAOHelper.query(ryaDAO, null, RDF.TYPE, OWL.SYMMETRICPROPERTY, conf);
            Set<URI> symProp = new HashSet();
            try {
                while (iter.hasNext()) {
                    Statement st = iter.next();
                    symProp.add((URI) st.getSubject()); //safe to assume it is a URI?
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            symmetricPropertySet = symProp;

            iter = RyaDAOHelper.query(ryaDAO, null, RDF.TYPE, OWL.TRANSITIVEPROPERTY, conf);
            Set<URI> transProp = new HashSet();
            try {
                while (iter.hasNext()) {
                    Statement st = iter.next();
                    transProp.add((URI) st.getSubject());
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            transitivePropertySet = transProp;

            iter = RyaDAOHelper.query(ryaDAO, null, OWL.INVERSEOF, null, conf);
            Map<URI, URI> invProp = new HashMap();
            try {
                while (iter.hasNext()) {
                    Statement st = iter.next();
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
            		vf.createURI("http://www.w3.org/2002/07/owl#propertyChainAxiom"),
            		null, conf);
            Map<URI,URI> propertyChainPropertiesToBNodes = new HashMap<URI, URI>();
            propertyChainPropertyToChain = new HashMap<URI, List<URI>>();
            try {
            	while (iter.hasNext()){
            		Statement st = iter.next();
            		propertyChainPropertiesToBNodes.put((URI)st.getSubject(), (URI)st.getObject());
            	}
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }
            // now for each property chain bNode, get the indexed list of properties associated with that chain
            for (URI propertyChainProperty : propertyChainPropertiesToBNodes.keySet()){
            	URI bNode = propertyChainPropertiesToBNodes.get(propertyChainProperty);
            	// query for the list of indexed properties
            	iter = RyaDAOHelper.query(ryaDAO, bNode, vf.createURI("http://www.w3.org/2000/10/swap/list#index"),
            			null, conf);
            	TreeMap<Integer, URI> orderedProperties = new TreeMap<Integer, URI>();
            	// TODO refactor this.  Wish I could execute sparql
            	try {
            		while (iter.hasNext()){
            		  Statement st = iter.next();
            		  String indexedElement = st.getObject().stringValue();
            		  System.out.println(indexedElement);
            		  CloseableIteration<Statement, QueryEvaluationException>  iter2 = RyaDAOHelper.query(ryaDAO, vf.createURI(st.getObject().stringValue()), RDF.FIRST,
                    			null, conf);
            		  String integerValue = "";
            		  Value anonPropNode = null;
            		  Value propURI = null;
            		  if (iter2 != null){
            			  while (iter2.hasNext()){
            				  Statement iter2Statement = iter2.next();
            				  integerValue = iter2Statement.getObject().stringValue();
            				  break;
            			  }
            			  iter2.close();
            		  }
            		  iter2 = RyaDAOHelper.query(ryaDAO, vf.createURI(st.getObject().stringValue()), RDF.REST,
                  			null, conf);
            		  if (iter2 != null){
            			  while (iter2.hasNext()){
            				  Statement iter2Statement = iter2.next();
            				  anonPropNode = iter2Statement.getObject();
            				  break;
            			  }
            			  iter2.close();
            			  if (anonPropNode != null){
            				  iter2 = RyaDAOHelper.query(ryaDAO, vf.createURI(anonPropNode.stringValue()), RDF.FIRST,
                            			null, conf);
            				  while (iter2.hasNext()){
                				  Statement iter2Statement = iter2.next();
                				  propURI = iter2Statement.getObject();
                				  break;
                			  }
                			  iter2.close();
            			  }
            		  }
            		  if (!integerValue.isEmpty() && propURI!=null) {
            			  try {
                			  int indexValue = Integer.parseInt(integerValue);
                			  URI chainPropURI = vf.createURI(propURI.stringValue());
                			  orderedProperties.put(indexValue, chainPropURI);
            			  }
            			  catch (Exception ex){
            				  // TODO log an error here
            				  
            			  }
            		  }
            		}
            	} finally{
            		if (iter != null){
            			iter.close();
            		}
            	}
            	List<URI> properties = new ArrayList<URI>();
            	for (Map.Entry<Integer, URI> entry : orderedProperties.entrySet()){
            		properties.add(entry.getValue());
            	}
            	propertyChainPropertyToChain.put(propertyChainProperty, properties);
            }
            
            // could also be represented as a list of properties (some of which may be blank nodes)
            for (URI propertyChainProperty : propertyChainPropertiesToBNodes.keySet()){
            	List<URI> existingChain = propertyChainPropertyToChain.get(propertyChainProperty);
            	// if we didn't get a chain, try to get it through following the collection
            	if ((existingChain == null) || existingChain.isEmpty()) {
            		
          		  CloseableIteration<Statement, QueryEvaluationException>  iter2 = RyaDAOHelper.query(ryaDAO, propertyChainPropertiesToBNodes.get(propertyChainProperty), RDF.FIRST,
              			null, conf);
          		  List<URI> properties = new ArrayList<URI>();
          		  URI previousBNode = propertyChainPropertiesToBNodes.get(propertyChainProperty);
            	  if (iter2.hasNext()) {
            		  Statement iter2Statement = iter2.next();
            		  Value currentPropValue = iter2Statement.getObject();
            		  while ((currentPropValue != null) && (!currentPropValue.stringValue().equalsIgnoreCase(RDF.NIL.stringValue()))){
                		  if (currentPropValue instanceof URI){
                    		  iter2 = RyaDAOHelper.query(ryaDAO, vf.createURI(currentPropValue.stringValue()), RDF.FIRST,
                          			null, conf);
                			  if (iter2.hasNext()){
                				  iter2Statement = iter2.next();
                				  if (iter2Statement.getObject() instanceof URI){
                					  properties.add((URI)iter2Statement.getObject());
                				  }
                			  }
                			  // otherwise see if there is an inverse declaration
                			  else {
                				  iter2 = RyaDAOHelper.query(ryaDAO, vf.createURI(currentPropValue.stringValue()), OWL.INVERSEOF,
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

        } catch (QueryEvaluationException e) {
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
    private void addPredicateEdges(URI predicate, Direction dir, Graph graph, String edgeName)
            throws QueryEvaluationException {
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO,
                null, predicate, null, conf);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
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
        Map<URI, Set<URI>> domainByTypePartial = new ConcurrentHashMap<>();
        Map<URI, Set<URI>> rangeByTypePartial = new ConcurrentHashMap<>();
        // First, populate domain and range based on direct domain/range triples.
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, RDFS.DOMAIN, null, conf);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                Resource property = st.getSubject();
                Value domainType = st.getObject();
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
                Statement st = iter.next();
                Resource property = st.getSubject();
                Value rangeType = st.getObject();
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
        Set<URI> domainRangeTypeSet = new HashSet<>(domainByTypePartial.keySet());
        domainRangeTypeSet.addAll(rangeByTypePartial.keySet());
        // Extend to subproperties: make sure that using a more specific form of a property
        // still triggers its domain/range inferences.
        // Mirror for inverse properties: make sure that using the inverse form of a property
        // triggers the inverse domain/range inferences.
        // These two rules can recursively trigger one another.
        for (URI domainRangeType : domainRangeTypeSet) {
            Set<URI> propertiesWithDomain = domainByTypePartial.getOrDefault(domainRangeType, new HashSet<>());
            Set<URI> propertiesWithRange = rangeByTypePartial.getOrDefault(domainRangeType, new HashSet<>());
            // Since findParents will traverse the subproperty graph and find all indirect
            // subproperties, the subproperty rule does not need to trigger itself directly.
            // And since no more than one inverseOf relationship is stored for any property, the
            // inverse property rule does not need to trigger itself directly. However, each rule
            // can trigger the other, so keep track of how the inferred domains/ranges were
            // discovered so we can apply only those rules that might yield new information.
            Stack<URI> domainViaSuperProperty  = new Stack<>();
            Stack<URI> rangeViaSuperProperty  = new Stack<>();
            Stack<URI> domainViaInverseProperty  = new Stack<>();
            Stack<URI> rangeViaInverseProperty  = new Stack<>();
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
                    URI property = domainViaSuperProperty.pop();
                    URI inverseProperty = findInverseOf(property);
                    if (inverseProperty != null && propertiesWithRange.add(inverseProperty)) {
                        rangeViaInverseProperty.push(inverseProperty);
                    }
                }
                // For a type c and property p, if c is a range of p, then c is the domain of any
                // inverse of p. Would be redundant for properties discovered via inverseOf.
                while (!rangeViaSuperProperty.isEmpty()) {
                    URI property = rangeViaSuperProperty.pop();
                    URI inverseProperty = findInverseOf(property);
                    if (inverseProperty != null && propertiesWithDomain.add(inverseProperty)) {
                        domainViaInverseProperty.push(inverseProperty);
                    }
                }
                // For a type c and property p, if c is a domain of p, then c is also a domain of
                // p's subproperties. Would be redundant for properties discovered via this rule.
                while (!domainViaInverseProperty.isEmpty()) {
                    URI property = domainViaInverseProperty.pop();
                    Set<URI> subProperties = findParents(subPropertyOfGraph, property);
                    subProperties.removeAll(propertiesWithDomain);
                    propertiesWithDomain.addAll(subProperties);
                    domainViaSuperProperty.addAll(subProperties);
                }
                // For a type c and property p, if c is a range of p, then c is also a range of
                // p's subproperties. Would be redundant for properties discovered via this rule.
                while (!rangeViaInverseProperty.isEmpty()) {
                    URI property = rangeViaInverseProperty.pop();
                    Set<URI> subProperties = findParents(subPropertyOfGraph, property);
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
        for (URI subtype : domainRangeTypeSet) {
            Set<URI> supertypes = findChildren(subClassOfGraph, subtype);
            Set<URI> propertiesWithDomain = domainByTypePartial.getOrDefault(subtype, new HashSet<>());
            Set<URI> propertiesWithRange = rangeByTypePartial.getOrDefault(subtype, new HashSet<>());
            for (URI supertype : supertypes) {
                // For a property p and its domain c: all of c's superclasses are also domains of p.
                if (!propertiesWithDomain.isEmpty() && !domainByTypePartial.containsKey(supertype)) {
                    domainByTypePartial.put(supertype, new HashSet<>());
                }
                for (URI property : propertiesWithDomain) {
                    domainByTypePartial.get(supertype).add(property);
                }
                // For a property p and its range c: all of c's superclasses are also ranges of p.
                if (!propertiesWithRange.isEmpty() && !rangeByTypePartial.containsKey(supertype)) {
                    rangeByTypePartial.put(supertype, new HashSet<>());
                }
                for (URI property : propertiesWithRange) {
                    rangeByTypePartial.get(supertype).add(property);
                }
            }
        }
        domainByType = domainByTypePartial;
        rangeByType = rangeByTypePartial;
    }

    private void refreshPropertyRestrictions() throws QueryEvaluationException {
        // Get a set of all property restrictions of any type
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.ONPROPERTY, null, conf);
        Map<Resource, URI> restrictions = new HashMap<>();
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
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
    }

    private void refreshHasValueRestrictions(Map<Resource, URI> restrictions) throws QueryEvaluationException {
        hasValueByType = new HashMap<>();
        hasValueByProperty = new HashMap<>();
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.HASVALUE, null, conf);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                Resource restrictionClass = st.getSubject();
                if (restrictions.containsKey(restrictionClass)) {
                    URI property = restrictions.get(restrictionClass);
                    Value value = st.getObject();
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

    private void refreshAllValuesFromRestrictions(Map<Resource, URI> restrictions) throws QueryEvaluationException {
        allValuesFromByValueType = new HashMap<>();
        CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null, OWL.ALLVALUESFROM, null, conf);
        try {
            while (iter.hasNext()) {
                Statement st = iter.next();
                if (restrictions.containsKey(st.getSubject()) && st.getObject() instanceof URI) {
                    URI property = restrictions.get(st.getSubject());
                    URI valueClass = (URI) st.getObject();
                    // Should also be triggered by subclasses of the property restriction
                    Set<Resource> restrictionClasses = new HashSet<>();
                    restrictionClasses.add(st.getSubject());
                    if (st.getSubject() instanceof URI) {
                        restrictionClasses.addAll(findParents(subClassOfGraph, (URI) st.getSubject()));
                    }
                    for (Resource restrictionClass : restrictionClasses) {
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

    private static Vertex getVertex(Graph graph, Object id) {
        Iterator<Vertex> it = graph.vertices(id.toString());
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    private void addStatementEdge(Graph graph, String edgeName, Statement st) {
        Resource subj = st.getSubject();
        Vertex a = getVertex(graph, subj);
        if (a == null) {
            a = graph.addVertex(T.id, subj.toString());
            a.property(URI_PROP, subj);
        }
        Value obj = st.getObject();
        Vertex b = getVertex(graph, obj);
        if (b == null) {
            b = graph.addVertex(T.id, obj.toString());
            b.property(URI_PROP, obj);
        }
        a.addEdge(edgeName, b);
   }

    public Set<URI> findParents(Graph graph, URI vertexId) {
        return findConnected(graph, vertexId, Direction.IN);
    }

    public Set<URI> findChildren(Graph graph, URI vertexId) {
        return findConnected(graph, vertexId, Direction.OUT);
    }

    private Set<URI> findConnected(Graph graph, URI vertexId, Direction traversal) {
        Set<URI> connected = new HashSet<>();
        if (graph == null) {
            return connected;
        }
        Vertex v = getVertex(graph, vertexId);
        if (v == null) {
            return connected;
        }
        addConnected(v, connected, traversal);
        return connected;
    }

    private static void addConnected(Vertex v, Set<URI> connected, Direction traversal) {
        v.edges(traversal).forEachRemaining(edge -> {
            Vertex ov = edge.vertices(traversal.opposite()).next();
            Object o = ov.property(URI_PROP).value();
            if (o != null && o instanceof URI) {
                boolean contains = connected.contains(o);
                if (!contains) {
                    connected.add((URI) o);
                    addConnected(ov, connected, traversal);
                }
            }
        });
    }

    public boolean isSymmetricProperty(URI prop) {
        return (symmetricPropertySet != null) && symmetricPropertySet.contains(prop);
    }

    public URI findInverseOf(URI prop) {
        return (inverseOfMap != null) ? inverseOfMap.get(prop) : (null);
    }

    public boolean isTransitiveProperty(URI prop) {
        return (transitivePropertySet != null) && transitivePropertySet.contains(prop);
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public Set<Statement> findTransitiveProperty(Resource subj, URI prop, Value obj, Resource... contxts) throws InferenceEngineException {
        if (transitivePropertySet.contains(prop)) {
            Set<Statement> sts = new HashSet();
            boolean goUp = subj == null;
            chainTransitiveProperty(subj, prop, obj, (goUp) ? (obj) : (subj), sts, goUp, contxts);
            return sts;
        } else
            return null;
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public Set<Resource> findSameAs(Resource value, Resource... contxts) throws InferenceEngineException{
		Set<Resource> sameAs = new HashSet<Resource>();
		sameAs.add(value);
		findSameAsChaining(value, sameAs, contxts);
		return sameAs;
    }

    /**
     * TODO: This chaining can be slow at query execution. the other option is to perform this in the query itself, but that will be constrained to how many levels we decide to go
     */
    public void findSameAsChaining(Resource subj, Set<Resource> currentSameAs, Resource[] contxts) throws InferenceEngineException{
        try {
			CloseableIteration<Statement, QueryEvaluationException> subjIter = RyaDAOHelper.query(ryaDAO, subj, OWL.SAMEAS, null, conf, contxts);
			while (subjIter.hasNext()){
				Statement st = subjIter.next();
				if (!currentSameAs.contains(st.getObject())){
					Resource castedObj = (Resource) st.getObject();
					currentSameAs.add(castedObj);
					findSameAsChaining(castedObj, currentSameAs, contxts);
				}
			}
			subjIter.close();
			CloseableIteration<Statement, QueryEvaluationException> objIter = RyaDAOHelper.query(ryaDAO, null, OWL.SAMEAS, subj, conf, contxts);
			while (objIter.hasNext()){
				Statement st = objIter.next();
				if (!currentSameAs.contains(st.getSubject())){
					Resource sameAsSubj = st.getSubject();
					currentSameAs.add(sameAsSubj);
					findSameAsChaining(sameAsSubj, currentSameAs, contxts);
				}
			}
			objIter.close();
		} catch (QueryEvaluationException e) {
			throw new InferenceEngineException(e);
		}

    }

    protected void chainTransitiveProperty(Resource subj, URI prop, Value obj, Value core, Set<Statement> sts, boolean goUp, Resource[] contxts) throws InferenceEngineException {
        try {
            CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, subj, prop, obj, conf, contxts);
            while (iter.hasNext()) {
                Statement st = iter.next();
                sts.add(new StatementImpl((goUp) ? (st.getSubject()) : (Resource) (core), prop, (!goUp) ? (st.getObject()) : (core)));
                if (goUp) {
                    chainTransitiveProperty(null, prop, st.getSubject(), core, sts, goUp, contxts);
                } else {
                    chainTransitiveProperty((Resource) st.getObject(), prop, null, core, sts, goUp, contxts);
                }
            }
            iter.close();
        } catch (QueryEvaluationException e) {
            throw new InferenceEngineException(e);
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public RyaDAO getRyaDAO() {
        return ryaDAO;
    }

    public void setRyaDAO(RyaDAO ryaDAO) {
        this.ryaDAO = ryaDAO;
    }

    public RdfCloudTripleStoreConfiguration getConf() {
        return conf;
    }

    public void setConf(RdfCloudTripleStoreConfiguration conf) {
        this.conf = conf;
    }

    public Graph getSubClassOfGraph() {
        return subClassOfGraph;
    }

    public Map<URI, List<URI>> getPropertyChainMap() {
        return propertyChainPropertyToChain;
    }

    public List<URI> getPropertyChain(URI chainProp) {
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

    public void setRefreshGraphSchedule(long refreshGraphSchedule) {
        this.refreshGraphSchedule = refreshGraphSchedule;
    }

    public Set<URI> getSymmetricPropertySet() {
        return symmetricPropertySet;
    }

    public void setSymmetricPropertySet(Set<URI> symmetricPropertySet) {
        this.symmetricPropertySet = symmetricPropertySet;
    }

    public Map<URI, URI> getInverseOfMap() {
        return inverseOfMap;
    }

    public void setInverseOfMap(Map<URI, URI> inverseOfMap) {
        this.inverseOfMap = inverseOfMap;
    }

    public Set<URI> getTransitivePropertySet() {
        return transitivePropertySet;
    }

    public void setTransitivePropertySet(Set<URI> transitivePropertySet) {
        this.transitivePropertySet = transitivePropertySet;
    }

    public boolean isSchedule() {
        return schedule;
    }

    public void setSchedule(boolean schedule) {
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
    public Map<URI, Set<Value>> getHasValueByType(Resource type) {
        Map<URI, Set<Value>> implications = new HashMap<>();
        if (hasValueByType != null) {
            Set<Resource> types = new HashSet<>();
            types.add(type);
            if (type instanceof URI) {
                types.addAll(findParents(subClassOfGraph, (URI) type));
            }
            for (Resource relevantType : types) {
                if (hasValueByType.containsKey(relevantType)) {
                    for (Map.Entry<URI, Value> propertyToValue : hasValueByType.get(relevantType).entrySet()) {
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
    public Map<Resource, Set<Value>> getHasValueByProperty(URI property) {
        Map<Resource, Set<Value>> implications = new HashMap<>();
        if (hasValueByProperty != null && hasValueByProperty.containsKey(property)) {
            for (Map.Entry<Resource, Value> typeToValue : hasValueByProperty.get(property).entrySet()) {
                Resource type = typeToValue.getKey();
                if (!implications.containsKey(type)) {
                    implications.put(type, new HashSet<>());
                }
                implications.get(type).add(typeToValue.getValue());
                if (type instanceof URI) {
                    for (URI subtype : findParents(subClassOfGraph, (URI) type)) {
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
    public Set<URI> getPropertiesWithDomain(URI domainType) {
        Set<URI> properties = new HashSet<>();
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
    public Set<URI> getPropertiesWithRange(URI rangeType) {
        Set<URI> properties = new HashSet<>();
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
    public Map<Resource, Set<URI>> getAllValuesFromByValueType(Resource valueType) {
        Map<Resource, Set<URI>> implications = new HashMap<>();
        if (allValuesFromByValueType != null) {
            // Check for any subtypes which would in turn imply the value type
            HashSet<Resource> valueTypes = new HashSet<>();
            valueTypes.add(valueType);
            if (valueType instanceof URI) {
                valueTypes.addAll(findParents(subClassOfGraph, (URI) valueType));
            }
            for (Resource valueSubType : valueTypes) {
                if (allValuesFromByValueType.containsKey(valueSubType)) {
                    Map<Resource, URI> restrictionToProperty = allValuesFromByValueType.get(valueSubType);
                    for (Resource restrictionType : restrictionToProperty.keySet()) {
                        if (!implications.containsKey(restrictionType)) {
                            implications.put(restrictionType, new HashSet<>());
                        }
                        URI property = restrictionToProperty.get(restrictionType);
                        implications.get(restrictionType).add(property);
                        // Also add subproperties that would in turn imply the property
                        implications.get(restrictionType).addAll(findParents(subPropertyOfGraph, property));
                    }
                }
            }
        }
        return implications;
    }
}
