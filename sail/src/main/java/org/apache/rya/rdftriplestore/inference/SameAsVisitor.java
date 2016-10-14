package org.apache.rya.rdftriplestore.inference;

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



import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.apache.rya.rdftriplestore.utils.TransitivePropertySP;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * All predicates are changed
 * Class SubPropertyOfVisitor
 * Date: Mar 29, 2011
 * Time: 11:28:34 AM
 */
public class SameAsVisitor extends AbstractInferVisitor {

    public SameAsVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferSubPropertyOf(); // oops
    }
    
    public void meet(StatementPattern sp) throws Exception {
        if (!include) {
            return;
        }
        if (sp instanceof FixedStatementPattern || sp instanceof TransitivePropertySP || sp instanceof DoNotExpandSP) {
            return;   //already inferred somewhere else
        }
        final Var predVar = sp.getPredicateVar();
        //do not know when things are null
        if (predVar == null) {
            return;
        }
        meetSP(sp);
    }

    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        StatementPattern sp = node.clone();
        final Var predVar = sp.getPredicateVar();

        boolean shouldExpand = true;
        if (predVar.hasValue()){
            URI pred = (URI) predVar.getValue();
            String predNamespace = pred.getNamespace();
            shouldExpand = !pred.equals(OWL.SAMEAS) && 
            !RDF.NAMESPACE.equals(predNamespace) &&
            !SESAME.NAMESPACE.equals(predNamespace) &&
            !RDFS.NAMESPACE.equals(predNamespace);
        }

        final Var objVar = sp.getObjectVar();
        final Var subjVar = sp.getSubjectVar();
        final Var cntxtVar = sp.getContextVar();
        if (shouldExpand
                && !EXPANDED.equals(cntxtVar) && !(objVar == null) && !(subjVar == null)){
            if (objVar.getValue() == null) {
            	Value subjVarValue = subjVar.getValue();
            	if (subjVarValue instanceof Resource){
            		Set<Resource> uris = inferenceEngine.findSameAs((Resource)subjVar.getValue(), getVarValue(cntxtVar));
            		if (uris.size() > 1){
            			InferJoin join = getReplaceJoin(uris, true, subjVar, objVar, predVar, cntxtVar);
            			node.replaceWith(join);  
            		}
            	}
            }
            else if (subjVar.getValue() == null) {
            	Value objVarValue = objVar.getValue();
            	if (objVarValue instanceof Resource){
            		Set<Resource> uris = inferenceEngine.findSameAs((Resource)objVar.getValue(), getVarValue(cntxtVar));
                	if (uris.size() > 1){
                        InferJoin join = getReplaceJoin(uris, false, subjVar, objVar, predVar, cntxtVar);
                        node.replaceWith(join);  
                	}
            	}  	
            }
            else {
            	// both subj and pred are set and should be expanded
            	Set<Resource> subjURIs = new HashSet<Resource>();
            	Set<Resource> objURIs = new HashSet<Resource>();
            	// TODO I don't like these checks -- is there a better way to do this?
            	Value objVarValue = objVar.getValue();
           	    if (objVarValue instanceof Resource){
           	    	objURIs = inferenceEngine.findSameAs((Resource)objVar.getValue(), getVarValue(cntxtVar));
            	}
            	Value subjVarValue = subjVar.getValue();
            	if (subjVarValue instanceof Resource){
            		subjURIs = inferenceEngine.findSameAs((Resource)subjVar.getValue(), getVarValue(cntxtVar));
            	}
            	InferJoin finalJoin = null;
            	// expand subj first
            	if (subjURIs.size() > 1){
            		finalJoin = getReplaceJoin(subjURIs, true, subjVar, objVar, predVar, cntxtVar);
            	}
            	// now expand the obj
            	if (objURIs.size() > 1){
            		// if we already expanded the subj
            		if (finalJoin != null){
            			// we know what this is since we created it
            			DoNotExpandSP origStatement = (DoNotExpandSP) finalJoin.getRightArg();
            	        String s = UUID.randomUUID().toString();
            	        Var dummyVar = new Var(s);
            			StatementPattern origDummyStatement = new DoNotExpandSP(origStatement.getSubjectVar(), origStatement.getPredicateVar(), dummyVar, cntxtVar);
            	        FixedStatementPattern fsp = new FixedStatementPattern(dummyVar, new Var("c-" + s, OWL.SAMEAS), objVar, cntxtVar);
            	        for (Resource sameAs : objURIs){
            	    		NullableStatementImpl newStatement = new NullableStatementImpl(sameAs, OWL.SAMEAS, (Resource)objVar.getValue(), getVarValue(cntxtVar));
            	            fsp.statements.add(newStatement);        		
            	    	}
            	        InferJoin interimJoin = new InferJoin(fsp, origDummyStatement);
            	        finalJoin = new InferJoin(finalJoin.getLeftArg(), interimJoin);
            		}
            		else {
            			finalJoin = getReplaceJoin(objURIs, false, subjVar, objVar, predVar, cntxtVar);
            		}
            		
            	}
            	if (finalJoin != null){
            	    node.replaceWith(finalJoin);
            	}
            }
        }
    }
    
    private InferJoin getReplaceJoin(Set<Resource> uris, boolean subSubj, Var subjVar, Var objVar, Var predVar, Var cntxtVar){
        String s = UUID.randomUUID().toString();
        Var dummyVar = new Var(s);
        StatementPattern origStatement;
        Var subVar;
        if (subSubj){
        	subVar = subjVar;
        	origStatement = new DoNotExpandSP(dummyVar, predVar, objVar, cntxtVar);
        }
        else {
        	subVar = objVar;
        	origStatement = new DoNotExpandSP(subjVar, predVar, dummyVar, cntxtVar);
       }
        FixedStatementPattern fsp = new FixedStatementPattern(dummyVar, new Var("c-" + s, OWL.SAMEAS), subVar, cntxtVar);
        for (Resource sameAs : uris){
    		NullableStatementImpl newStatement = new NullableStatementImpl(sameAs, OWL.SAMEAS, (Resource)subVar.getValue(), getVarValue(cntxtVar));
            fsp.statements.add(newStatement);        		
    	}
        InferJoin join = new InferJoin(fsp, origStatement);
        join.getProperties().put(InferConstants.INFERRED, InferConstants.TRUE);
       return join;
    }
    
    protected Resource getVarValue(Var var) {
        if (var == null)
            return null;
        else
            return (Resource)var.getValue();
    }

}
