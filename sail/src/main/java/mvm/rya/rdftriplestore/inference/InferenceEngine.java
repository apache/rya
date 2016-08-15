package mvm.rya.rdftriplestore.inference;

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



import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.utils.RyaDAOHelper;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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

    private RyaDAO ryaDAO;
    private RdfCloudTripleStoreConfiguration conf;
    private boolean initialized = false;
    private boolean schedule = true;

    private long refreshGraphSchedule = 5 * 60 * 1000; //5 min
    private Timer timer;
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
        try {
            //get all subclassof
            Graph graph = TinkerGraphFactory.createTinkerGraph();
            CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDAO, null,
                    RDFS.SUBCLASSOF, null, conf);
            try {
                while (iter.hasNext()) {
                    String edgeName = RDFS.SUBCLASSOF.stringValue();
                    Statement st = iter.next();
                    addStatementEdge(graph, edgeName, st);
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }

            subClassOfGraph = graph; //TODO: Should this be synchronized?

            graph = TinkerGraphFactory.createTinkerGraph();

            iter = RyaDAOHelper.query(ryaDAO, null,
                    RDFS.SUBPROPERTYOF, null, conf);
            try {
                while (iter.hasNext()) {
                    String edgeName = RDFS.SUBPROPERTYOF.stringValue();
                    Statement st = iter.next();
                    addStatementEdge(graph, edgeName, st);
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }

            //equiv property really is the same as a subPropertyOf both ways
            iter = RyaDAOHelper.query(ryaDAO, null, OWL.EQUIVALENTPROPERTY, null, conf);
            try {
                while (iter.hasNext()) {
                    String edgeName = RDFS.SUBPROPERTYOF.stringValue();
                    Statement st = iter.next();
                    addStatementEdge(graph, edgeName, st);
                    //reverse is also true
                    addStatementEdge(graph, edgeName, new StatementImpl((Resource) st.getObject(), st.getPredicate(), st.getSubject()));
                }
            } finally {
                if (iter != null) {
                    iter.close();
                }
            }

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
        } catch (QueryEvaluationException e) {
            throw new InferenceEngineException(e);
        }
    }

    protected void addStatementEdge(Graph graph, String edgeName, Statement st) {
        Resource subj = st.getSubject();
        Vertex a = graph.getVertex(subj);
        if (a == null) {
            a = graph.addVertex(subj);
            a.setProperty(URI_PROP, subj);
        }
        Value obj = st.getObject();
        Vertex b = graph.getVertex(obj);
        if (b == null) {
            b = graph.addVertex(obj);
            b.setProperty(URI_PROP, obj);
        }
        graph.addEdge(null, a, b, edgeName);
    }

    public Set<URI> findParents(Graph graph, URI vertexId) {
        Set<URI> parents = new HashSet();
        if (graph == null) {
            return parents;
        }
        Vertex v = graph.getVertex(vertexId);
        if (v == null) {
            return parents;
        }
        addParents(v, parents);
        return parents;
    }

    private static void addParents(Vertex v, Set<URI> parents) {
        for (Edge edge : v.getEdges(Direction.IN)) {
            Vertex ov = edge.getVertex(Direction.OUT);
            Object o = ov.getProperty(URI_PROP);
            if (o != null && o instanceof URI) {
                boolean contains = parents.contains(o);
                if (!contains) {
                    parents.add((URI) o);
                    addParents(ov, parents);
                }
            }

        }
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
				if (!currentSameAs.contains(st.getObject()) && (st.getObject() instanceof Resource) ){
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
}
