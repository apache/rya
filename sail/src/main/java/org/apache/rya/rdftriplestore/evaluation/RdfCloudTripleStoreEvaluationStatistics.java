package org.apache.rya.rdftriplestore.evaluation;

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



import static com.google.common.base.Preconditions.checkNotNull;
//import static RdfCloudTripleStoreUtils.getTtlValueConverter;



import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;

/**
 * Class RdfCloudTripleStoreEvaluationStatistics
 * Date: Apr 12, 2011
 * Time: 1:31:05 PM
 */
public class RdfCloudTripleStoreEvaluationStatistics extends EvaluationStatistics {

    private RdfCloudTripleStoreConfiguration conf;
    private RdfEvalStatsDAO rdfEvalStatsDAO;
    protected boolean pushEmptyRdfTypeDown = true;
    protected boolean useCompositeCardinalities = true;

    public RdfCloudTripleStoreEvaluationStatistics(RdfCloudTripleStoreConfiguration conf, RdfEvalStatsDAO rdfEvalStatsDAO) {
        checkNotNull(conf);
        checkNotNull(rdfEvalStatsDAO);
        try {
            this.conf = conf;
            this.rdfEvalStatsDAO = rdfEvalStatsDAO;
            pushEmptyRdfTypeDown = conf.isStatsPushEmptyRdftypeDown();
            useCompositeCardinalities = conf.isUseCompositeCardinality();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double getCardinality(TupleExpr expr) {
        if (expr instanceof Filter) {
            Filter f = (Filter) expr;
            // filters must make sets smaller
            return super.getCardinality(f.getArg()) / 10;
        }
        return super.getCardinality(expr);
    }

    @Override
    protected CardinalityCalculator createCardinalityCalculator() {
        return new RdfCloudTripleStoreCardinalityCalculator(this);
    }

    public RdfEvalStatsDAO getRdfEvalStatsDAO() {
        return rdfEvalStatsDAO;
    }

    public void setRdfEvalStatsDAO(RdfEvalStatsDAO rdfEvalStatsDAO) {
        this.rdfEvalStatsDAO = rdfEvalStatsDAO;
    }

    public class RdfCloudTripleStoreCardinalityCalculator extends CardinalityCalculator {
        private RdfCloudTripleStoreEvaluationStatistics statistics;
        protected Map<Var, Collection<Statement>> fspMap;

        public RdfCloudTripleStoreCardinalityCalculator(RdfCloudTripleStoreEvaluationStatistics statistics) {
            this.statistics = statistics;
        }
        
 
        @Override
        protected double getCardinality(StatementPattern sp) {
            Var subjectVar = sp.getSubjectVar();
            Resource subj = (Resource) getConstantValue(subjectVar);
            Var predicateVar = sp.getPredicateVar();
            URI pred = (URI) getConstantValue(predicateVar);
            Var objectVar = sp.getObjectVar();
            Value obj = getConstantValue(objectVar);
            Resource context = (Resource) getConstantValue(sp.getContextVar());

            // set rdf type to be a max value (as long as the object/subject aren't specified) to 
                if (pred != null) {
                    if (statistics.pushEmptyRdfTypeDown && RDF.TYPE.equals(pred) && subj == null && obj == null) {
                        return Double.MAX_VALUE;
                    }
                }

            // FixedStatementPattern indicates that this is when backward chaining reasoning is being used
            if (sp instanceof FixedStatementPattern) {
                //no query here
                FixedStatementPattern fsp = (FixedStatementPattern) sp;
                //TODO: assume that only the subject is open ended here
                Var fspSubjectVar = fsp.getSubjectVar();
                if (fspSubjectVar != null && fspSubjectVar.getValue() == null) {
                    if (fspMap == null) {
                        fspMap = new HashMap<Var, Collection<Statement>>();
                    }
                    fspMap.put(fspSubjectVar, fsp.statements);
                }
                return fsp.statements.size();
            }

            /**
             * Use the output of the FixedStatementPattern to determine more information
             */
            if (fspMap != null && sp instanceof DoNotExpandSP) {
                //TODO: Might be a better way than 3 map pulls
                RdfEvalStatsDAO.CARDINALITY_OF cardinality_of = null;
                Collection<Statement> statements = null;
                // TODO unsure of how to incorporate additional cardinalities here
                if (objectVar != null && objectVar.getValue() == null) {
                    statements = fspMap.get(objectVar);
                    cardinality_of = RdfEvalStatsDAO.CARDINALITY_OF.OBJECT;
                }
                if (statements == null && predicateVar != null && predicateVar.getValue() == null) {
                    statements = fspMap.get(predicateVar);
                    cardinality_of = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE;
                }
                if (statements == null && subjectVar != null && subjectVar.getValue() == null) {
                    statements = fspMap.get(subjectVar);
                    cardinality_of = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT;
                }
                if (statements != null) {
                    double fspCard = 0;
                    for (Statement statement : statements) {
                    	List<Value> values = new ArrayList<Value>();
                    	values.add(statement.getSubject());
                    	fspCard  += rdfEvalStatsDAO.getCardinality(conf, cardinality_of, values, context);
                    }
                    return fspCard;
                }
            }

            /**
             * We put full triple scans before rdf:type because more often than not
             * the triple scan is being joined with something else that is better than
             * asking the full rdf:type of everything.
             */
            double cardinality = Double.MAX_VALUE - 1;
            try {
                if (subj != null) {
                	List<Value> values = new ArrayList<Value>();
                	CARDINALITY_OF card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT;
            		values.add(subj);
            		if (useCompositeCardinalities){
                   	    if (pred != null){
                    		values.add(pred);
                    		card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTPREDICATE;
                    	}
                   	    else if (obj != null){
                    		values.add(obj);
                    		card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTOBJECT;
                   	    }
            		}
                	double evalCard = evalCard = rdfEvalStatsDAO.getCardinality(conf, card, values, context);
                	// the cardinality will be -1 if there was no value found (if the index does not exist)
                    if (evalCard >= 0) {
                        cardinality = Math.min(cardinality, evalCard);
                    } else {
                        cardinality = 1;
                    }
                }
                else if (pred != null) {
                	List<Value> values = new ArrayList<Value>();
                	CARDINALITY_OF card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE;
            		values.add(pred);
            		if (useCompositeCardinalities){
                   	    if (obj != null){
                    		values.add(obj);
                    		card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATEOBJECT;
                   	    }
            		}
                	double evalCard = evalCard = rdfEvalStatsDAO.getCardinality(conf, card, values, context);
                    if (evalCard >= 0) {
                        cardinality = Math.min(cardinality, evalCard);
                    } else {
                        cardinality = 1;
                    }
                }
                else if (obj != null) {
                	List<Value> values = new ArrayList<Value>();
            		values.add(obj);
                    double evalCard = rdfEvalStatsDAO.getCardinality(conf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values, context);
                    if (evalCard >= 0) {
                        cardinality = Math.min(cardinality, evalCard);
                    } else {
                        cardinality = 1;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return cardinality;
        }

        @Override
        protected void meetUnaryTupleOperator(UnaryTupleOperator node) {
            if (node instanceof Projection) {
                cardinality += -1.0;
            }
            super.meetUnaryTupleOperator(node);
        }

        @Override
        protected void meetBinaryTupleOperator(BinaryTupleOperator node) {
            node.getLeftArg().visit(this);
            double leftArgCost = cardinality;
            node.getRightArg().visit(this);
            cardinality += leftArgCost;
        }
        
        // TODO Is this sufficient for add capability of slice node?
        @Override
        public void meet(Slice node) {
            cardinality = node.getLimit();
        }
        

        @Override
        public void meet(Join node) {
            node.getLeftArg().visit(this);
            double leftArgCost = cardinality;
            node.getRightArg().visit(this);
            if (leftArgCost > cardinality) {
                cardinality = leftArgCost;    //TODO: Is this ok?
            }
        }

        protected Value getConstantValue(Var var) {
            if (var != null)
                return var.getValue();
            else
                return null;
        }
    }

}
