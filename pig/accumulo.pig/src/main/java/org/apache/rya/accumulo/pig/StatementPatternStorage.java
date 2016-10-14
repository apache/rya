package org.apache.rya.accumulo.pig;

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



import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;

import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

/**
 */
public class StatementPatternStorage extends AccumuloStorage {
    private static final Log logger = LogFactory.getLog(StatementPatternStorage.class);
    protected TABLE_LAYOUT layout;
    protected String subject = "?s";
    protected String predicate = "?p";
    protected String object = "?o";
    protected String context;
    private Value subject_value;
    private Value predicate_value;
    private Value object_value;

    private RyaTripleContext ryaContext;

    /**
     * whether to turn inferencing on or off
     */
    private boolean infer = false;

    public StatementPatternStorage() {
    	if (super.conf != null){
    		ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(super.conf));
    	}
    	else {
    		ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());
    	}
    	
    }

    private Value getValue(Var subjectVar) {
        return subjectVar.hasValue() ? subjectVar.getValue() : null;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        super.setLocation(location, job);
    }

    @Override
    protected void setLocationFromUri(String uri, Job job) throws IOException {
        super.setLocationFromUri(uri, job);
        // ex: accumulo://tablePrefix?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&subject=a&predicate=b&object=c&context=c&infer=true
        addStatementPatternRange(subject, predicate, object, context);
        if (infer) {
            addInferredRanges(table, job);
        }

        if (layout == null || ranges.size() == 0)
            throw new IllegalArgumentException("Range and/or layout is null. Check the query");
        table = RdfCloudTripleStoreUtils.layoutPrefixToTable(layout, table);
        tableName = new Text(table);
    }

    @Override
    protected void addLocationFromUriPart(String[] pair) {
        if (pair[0].equals("subject")) {
            this.subject = pair[1];
        } else if (pair[0].equals("predicate")) {
            this.predicate = pair[1];
        } else if (pair[0].equals("object")) {
            this.object = pair[1];
        } else if (pair[0].equals("context")) {
            this.context = pair[1];
        } else if (pair[0].equals("infer")) {
            this.infer = Boolean.parseBoolean(pair[1]);
        }
    }

    protected void addStatementPatternRange(String subj, String pred, String obj, String ctxt) throws IOException {
        logger.info("Adding statement pattern[subject:" + subj + ", predicate:" + pred + ", object:" + obj + ", context:" + ctxt + "]");
        StringBuilder sparqlBuilder = new StringBuilder();
        sparqlBuilder.append("select * where {\n");
        if (ctxt != null) {
            /**
             * select * where {
             GRAPH ?g {
             <http://www.example.org/exampleDocument#Monica> ?p ?o.
             }
             }
             */
            sparqlBuilder.append("GRAPH ").append(ctxt).append(" {\n");
        }
        sparqlBuilder.append(subj).append(" ").append(pred).append(" ").append(obj).append(".\n");
        if (ctxt != null) {
            sparqlBuilder.append("}\n");
        }
        sparqlBuilder.append("}\n");
        String sparql = sparqlBuilder.toString();

        if (logger.isDebugEnabled()) {
            logger.debug("Sparql statement range[" + sparql + "]");
        }

        QueryParser parser = new SPARQLParser();
        ParsedQuery parsedQuery = null;
        try {
            parsedQuery = parser.parseQuery(sparql, null);
        } catch (MalformedQueryException e) {
            throw new IOException(e);
        }
        parsedQuery.getTupleExpr().visitChildren(new QueryModelVisitorBase<IOException>() {
            @Override
            public void meet(StatementPattern node) throws IOException {
                Var subjectVar = node.getSubjectVar();
                Var predicateVar = node.getPredicateVar();
                Var objectVar = node.getObjectVar();
                subject_value = getValue(subjectVar);
                predicate_value = getValue(predicateVar);
                object_value = getValue(objectVar);
                Var contextVar = node.getContextVar();
                Map.Entry<TABLE_LAYOUT, Range> temp = createRange(subject_value, predicate_value, object_value);
                layout = temp.getKey();
                Range range = temp.getValue();
                addRange(range);
                if (contextVar != null && contextVar.getValue() != null) {
                    String context_str = contextVar.getValue().stringValue();
                    addColumnPair(context_str, "");
                }
            }
        });
    }

    protected Map.Entry<TABLE_LAYOUT, Range> createRange(Value s_v, Value p_v, Value o_v) throws IOException {
        RyaURI subject_rya = RdfToRyaConversions.convertResource((Resource) s_v);
        RyaURI predicate_rya = RdfToRyaConversions.convertURI((URI) p_v);
        RyaType object_rya = RdfToRyaConversions.convertValue(o_v);
        TriplePatternStrategy strategy = ryaContext.retrieveStrategy(subject_rya, predicate_rya, object_rya, null);
        if (strategy == null)
            return new RdfCloudTripleStoreUtils.CustomEntry<TABLE_LAYOUT, Range>(TABLE_LAYOUT.SPO, new Range());
        Map.Entry<TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(subject_rya, predicate_rya, object_rya, null, null);
        ByteRange byteRange = entry.getValue();
        return new RdfCloudTripleStoreUtils.CustomEntry<org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range>(
                entry.getKey(), new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()))
        );
    }

    protected void addInferredRanges(String tablePrefix, Job job) throws IOException {
        logger.info("Adding inferences to statement pattern[subject:" + subject_value + ", predicate:" + predicate_value + ", object:" + object_value + "]");
        //inference engine
        AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        InferenceEngine inferenceEngine = new InferenceEngine();
        try {
            AccumuloRdfConfiguration rdfConf = new AccumuloRdfConfiguration(job.getConfiguration());
            rdfConf.setTablePrefix(tablePrefix);
            ryaDAO.setConf(rdfConf);
            try {
                if (!mock) {
                    ryaDAO.setConnector(new ZooKeeperInstance(inst, zookeepers).getConnector(user, password.getBytes()));
                } else {
                    ryaDAO.setConnector(new MockInstance(inst).getConnector(user, password.getBytes()));
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
            ryaDAO.init();
            inferenceEngine.setConf(rdfConf);
            inferenceEngine.setRyaDAO(ryaDAO);
            inferenceEngine.setSchedule(false);
            inferenceEngine.init();
            //is it subclassof or subpropertyof
            if (RDF.TYPE.equals(predicate_value)) {
                //try subclassof
                Collection<URI> parents = inferenceEngine.findParents(inferenceEngine.getSubClassOfGraph(), (URI) object_value);
                if (parents != null && parents.size() > 0) {
                    //subclassof relationships found
                    //don't add self, that will happen anyway later
                    //add all relationships
                    for (URI parent : parents) {
                        Map.Entry<TABLE_LAYOUT, Range> temp =
                                createRange(subject_value, predicate_value, parent);
                        Range range = temp.getValue();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Found subClassOf relationship [type:" + object_value + " is subClassOf:" + parent + "]");
                        }
                        addRange(range);
                    }
                }
            } else if (predicate_value != null) {
                //subpropertyof check
                Set<URI> parents = inferenceEngine.findParents(inferenceEngine.getSubPropertyOfGraph(), (URI) predicate_value);
                for (URI parent : parents) {
                    Map.Entry<TABLE_LAYOUT, Range> temp =
                            createRange(subject_value, parent, object_value);
                    Range range = temp.getValue();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Found subPropertyOf relationship [type:" + predicate_value + " is subPropertyOf:" + parent + "]");
                    }
                    addRange(range);
                }
            }
        } catch (Exception e) {
            logger.error("Exception in adding inferred ranges", e);
            throw new IOException(e);
        } finally {
            if (inferenceEngine != null) {
                try {
                    inferenceEngine.destroy();
                } catch (InferenceEngineException e) {
                    logger.error("Exception closing InferenceEngine", e);
                }
            }
            if (ryaDAO != null) {
                try {
                    ryaDAO.destroy();
                } catch (RyaDAOException e) {
                    logger.error("Exception closing ryadao", e);
                }
            }
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (reader.nextKeyValue()) {
                Key key = (Key) reader.getCurrentKey();
                org.apache.accumulo.core.data.Value value = (org.apache.accumulo.core.data.Value) reader.getCurrentValue();
                ByteArrayDataInput input = ByteStreams.newDataInput(key.getRow().getBytes());
                RyaStatement ryaStatement = ryaContext.deserializeTriple(layout, new TripleRow(key.getRow().getBytes(),
                        key.getColumnFamily().getBytes(), key.getColumnQualifier().getBytes()));

                Tuple tuple = TupleFactory.getInstance().newTuple(7);
                tuple.set(0, ryaStatement.getSubject().getData());
                tuple.set(1, ryaStatement.getPredicate().getData());
                tuple.set(2, ryaStatement.getObject().getData());
                tuple.set(3, (ryaStatement.getContext() != null) ? (ryaStatement.getContext().getData()) : (null));
                tuple.set(4, ryaStatement.getSubject().getDataType());
                tuple.set(5, ryaStatement.getPredicate().getDataType());
                tuple.set(6, ryaStatement.getObject().getDataType());
                return tuple;
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return null;
    }
}
