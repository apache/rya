package org.apache.rya.joinselect;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfUtils;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.persist.RdfDAOException;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;





public class AccumuloSelectivityEvalDAO implements SelectivityEvalDAO<RdfCloudTripleStoreConfiguration> {

  private boolean initialized = false;
  private RdfCloudTripleStoreConfiguration conf;
  private Connector connector;
  private TableLayoutStrategy tableLayoutStrategy;
  private boolean filtered = false;
  private boolean denormalized = false;
  private int FullTableCardinality = 0;
  private static final String DELIM = "\u0000";
  private Map<String,Long> joinMap = new HashMap<String,Long>();;
  private RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> resd;

  @Override
  public void init() throws RdfDAOException {
    try {
      if (isInitialized()) {
        throw new IllegalStateException("Already initialized");
      }
      if (!resd.isInitialized()) {
        resd.init();
      }
      checkNotNull(connector);
      tableLayoutStrategy = conf.getTableLayoutStrategy();
      TableOperations tos = connector.tableOperations();
      AccumuloRdfUtils.createTableIfNotExist(tos, tableLayoutStrategy.getSelectivity());
      AccumuloRdfUtils.createTableIfNotExist(tos, tableLayoutStrategy.getProspects());
      initialized = true;
    } catch (Exception e) {
      throw new RdfDAOException(e);
    }
  }
  
  
  public AccumuloSelectivityEvalDAO() {
      
  }
  

  public AccumuloSelectivityEvalDAO(RdfCloudTripleStoreConfiguration conf, Connector connector) {

    this.conf = conf;
    this.connector = connector;
  }

  public AccumuloSelectivityEvalDAO(RdfCloudTripleStoreConfiguration conf) {
      
      this.conf = conf;
      Instance inst = new ZooKeeperInstance(conf.get("sc.cloudbase.instancename"), conf.get("sc.cloudbase.zookeepers"));
      try {
        this.connector = inst.getConnector(conf.get("sc.cloudbase.username"), conf.get("sc.cloudbase.password"));
    } catch (AccumuloException e) {
        e.printStackTrace();
    } catch (AccumuloSecurityException e) {
        e.printStackTrace();
    }
  }

  @Override
  public void destroy() throws RdfDAOException {
    if (!isInitialized()) {
      throw new IllegalStateException("Not initialized");
    }
    initialized = false;
  }

  @Override
  public boolean isInitialized() throws RdfDAOException {
    return initialized;
  }

  public Connector getConnector() {
    return connector;
  }

  public void setConnector(Connector connector) {
    this.connector = connector;
  }

  @Override
  public RdfCloudTripleStoreConfiguration getConf() {
    return conf;
  }

  @Override
  public void setConf(RdfCloudTripleStoreConfiguration conf) {
    this.conf = conf;
  }

  public RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> getRdfEvalDAO() {
    return resd;
  }

  public void setRdfEvalDAO(RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> resd) {
    this.resd = resd;
  }

  public void setFiltered(boolean filtered) {
    this.filtered = filtered;
  }
  
  
  public void setDenormalized(boolean denormalize) {
      this.denormalized = denormalize;
  }

  private double getJoinSelect(RdfCloudTripleStoreConfiguration conf, StatementPattern sp1, StatementPattern sp2) throws TableNotFoundException {

    if (FullTableCardinality == 0) {
      this.getTableSize(conf);
    }

    Authorizations authorizations = getAuths(conf);
    String row1 = CardinalityCalcUtil.getRow(sp1, true);
    String row2 = CardinalityCalcUtil.getRow(sp2, true);
    List<String> joinType = CardinalityCalcUtil.getJoins(sp1, sp2);

    if (joinType.size() == 0) {
      return 1;
    }

    if (joinType.size() == 2) {

      String cacheRow1;
      String cacheRow2;
      long card1 = 0;
      long card2 = 0;
      boolean contCard1 = false;
      boolean contCard2 = false;

      cacheRow1 = row1 + DELIM + joinType.get(0);
      cacheRow2 = row2 + DELIM + joinType.get(1);

      long count1 = getCardinality(conf, sp1);
      long count2 = getCardinality(conf, sp2);

      if (count1 == 0 || count2 == 0) {
        return 0;
      }

      if (joinMap.containsKey(cacheRow1)) {
        card1 = joinMap.get(cacheRow1);
        contCard1 = true;
      }
      if (joinMap.containsKey(cacheRow2)) {
        card2 = joinMap.get(cacheRow2);
        contCard2 = true;
      }

      if (!contCard1) {
        Scanner joinScanner = connector.createScanner(tableLayoutStrategy.getSelectivity(), authorizations);
        joinScanner.setRange(Range.prefix(row1));

        for (Map.Entry<Key,Value> entry : joinScanner) {
          if (entry.getKey().getColumnFamily().toString().equals(joinType.get(0))) {
            card1 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow1, card1);
            // System.out.println("Card1 is " + card1);
            break;
          }
        }
      }

      if (!contCard2) {
        Scanner joinScanner = connector.createScanner(tableLayoutStrategy.getSelectivity(), authorizations);
        joinScanner.setRange(Range.prefix(row2));
        for (Map.Entry<Key,Value> entry : joinScanner) {
          if (entry.getKey().getColumnFamily().toString().equals(joinType.get(1))) {
            card2 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow2, card2);
            // System.out.println("Card2 is " + card2);
            break;
          }
        }

      }

      if (!filtered && !denormalized) {
        double temp1 = Math.min(((double) card1) / ((double) count1 * FullTableCardinality), ((double) card2) / ((double) count2 * FullTableCardinality));

        double temp2 = Math.max((double) count1 / FullTableCardinality, (double) count2 / FullTableCardinality);

        // TODO maybe change back to original form as temp2 will rarely be less than temp1.
        return Math.min(temp1, temp2);
      } else if(denormalized) {
          return Math.min(card1,card2);
      } else {
      
        return Math.min(((double) card1 * count2) / ((double) count1 * FullTableCardinality * FullTableCardinality), ((double) card2 * count1)
            / ((double) count2 * FullTableCardinality * FullTableCardinality));

      }
    } else {

      String cacheRow1 = row1 + DELIM + joinType.get(0);
      String cacheRow2 = row1 + DELIM + joinType.get(1);
      String cacheRow3 = row2 + DELIM + joinType.get(2);
      String cacheRow4 = row2 + DELIM + joinType.get(3);
      long card1 = 0;
      long card2 = 0;
      long card3 = 0;
      long card4 = 0;
      boolean contCard1 = false;
      boolean contCard2 = false;

      long count1 = getCardinality(conf, sp1);
      long count2 = getCardinality(conf, sp2);

      if (count1 == 0 || count2 == 0) {
        return 0;
      }

      if (joinMap.containsKey(cacheRow1) && joinMap.containsKey(cacheRow2)) {
        card1 = joinMap.get(cacheRow1);
        card2 = joinMap.get(cacheRow2);
        contCard1 = true;
      }
      if (joinMap.containsKey(cacheRow3) && joinMap.containsKey(cacheRow4)) {
        card3 = joinMap.get(cacheRow3);
        card4 = joinMap.get(cacheRow4);
        contCard2 = true;
      }

      if (!contCard1) {
        Scanner joinScanner = connector.createScanner(tableLayoutStrategy.getSelectivity(), authorizations);
        joinScanner.setRange(Range.prefix(row1));
        boolean found1 = false;
        boolean found2 = false;

        for (Map.Entry<Key,Value> entry : joinScanner) {

          if (entry.getKey().getColumnFamily().toString().equals(joinType.get(0))) {
            card1 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow1, card1);
            found1 = true;
            // System.out.println("Card1 is " + card1);
            if (found1 && found2) {
              card1 = Math.min(card1, card2);
              break;
            }
          } else if (entry.getKey().getColumnFamily().toString().equals(joinType.get(1))) {
            card2 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow2, card2);
            found2 = true;
            // System.out.println("Card1 is " + card1);
            if (found1 && found2) {
              card1 = Math.min(card1, card2);
              break;
            }
          }
        }
      }

      if (!contCard2) {
        Scanner joinScanner = connector.createScanner(tableLayoutStrategy.getSelectivity(), authorizations);
        joinScanner.setRange(Range.prefix(row2));
        boolean found1 = false;
        boolean found2 = false;
        for (Map.Entry<Key,Value> entry : joinScanner) {
          if (entry.getKey().getColumnFamily().toString().equals(joinType.get(2))) {
            card3 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow3, card3);
            found1 = true;
            // System.out.println("Card2 is " + card2);
            if (found1 && found2) {
              card3 = Math.min(card3, card4);
              break;
            }
          } else if (entry.getKey().getColumnFamily().toString().equals(joinType.get(3))) {
            card4 = CardinalityCalcUtil.getJCard(entry.getKey());
            joinMap.put(cacheRow4, card4);
            found2 = true;
            // System.out.println("Card1 is " + card1);
            if (found1 && found2) {
              card3 = Math.min(card3, card4);
              break;
            }
          }
        }

      }

      if (!filtered && !denormalized) {
        return Math.min(((double) card1) / ((double) count1 * FullTableCardinality), ((double) card3) / ((double) count2 * FullTableCardinality));
      } else if(denormalized) { 
          return Math.min(card1,card3);
      } else {
        return Math.min(((double) card1 * count2) / ((double) count1 * FullTableCardinality * FullTableCardinality), ((double) card3 * count1)
            / ((double) count2 * FullTableCardinality * FullTableCardinality));

      }

    }

  }

  // TODO currently computes average selectivity of sp1 with each node in TupleExpr te (is this best?)
    private double getSpJoinSelect(RdfCloudTripleStoreConfiguration conf, TupleExpr te, StatementPattern sp1)
            throws TableNotFoundException {

        // System.out.println("Tuple is " + te + " and sp is " + sp1);

        if (te instanceof StatementPattern) {
            return getJoinSelect(conf, (StatementPattern) te, sp1);
        } else {

            SpExternalCollector spe = new SpExternalCollector();
            te.visit(spe);
            List<QueryModelNode> espList = spe.getSpExtTup();

            if (espList.size() == 0) {

                Set<String> tupBn = te.getAssuredBindingNames();
                Set<String> eBn = sp1.getAssuredBindingNames();
                Set<String> intersect = Sets.intersection(tupBn, eBn);

                return Math.pow(1.0 / 10000.0, intersect.size());

            }
            
            double min = Double.MAX_VALUE;
            double select = Double.MAX_VALUE;

            for (QueryModelNode node : espList) {

                if (node instanceof StatementPattern)
                    select = getJoinSelect(conf, sp1, (StatementPattern) node);
                else if (node instanceof ExternalSet) {
                    select = getExtJoinSelect(sp1, (ExternalSet) node);
                }

                if (min > select) {
                    min = select;
                }
            }
            // System.out.println("Max is " + max);
            return min;
        }
    }

  public double getJoinSelect(RdfCloudTripleStoreConfiguration conf, TupleExpr te1, TupleExpr te2) throws TableNotFoundException {

      SpExternalCollector spe = new SpExternalCollector();
      te2.visit(spe);
      List<QueryModelNode> espList = spe.getSpExtTup();  
      
      double min = Double.MAX_VALUE;

      for (QueryModelNode node : espList) {
        double select = getSelectivity(conf, te1, node);
        if (min > select) {
          min = select;
        }
      }

      return min;
    }

  
  
  
  private double getSelectivity(RdfCloudTripleStoreConfiguration conf, TupleExpr te, QueryModelNode node) throws TableNotFoundException {
      
        if ((node instanceof StatementPattern)) {
            return getSpJoinSelect(conf, te, (StatementPattern) node);

        } else if (node instanceof ExternalSet) {

            return getExtJoinSelect(te, (ExternalSet) node);

        } else {
            return 0;
        }
      
  }
  
  
 
  
  
  private double getExtJoinSelect(TupleExpr te, ExternalSet eSet) {
      
        Set<String> tupBn = te.getAssuredBindingNames();
        Set<String> eBn = eSet.getAssuredBindingNames();
        Set<String> intersect = Sets.intersection(tupBn, eBn);
       
        return Math.pow(1.0 / 10000.0, intersect.size());
      
  }
  
  
  
  
  
  
  
  
  
  

  // obtains cardinality for StatementPattern. Returns cardinality of 0
  // if no instances of constants occur in table.
  // assumes composite cardinalities will be used.
  @Override
  public long getCardinality(RdfCloudTripleStoreConfiguration conf, StatementPattern sp) throws TableNotFoundException {

    Var subjectVar = sp.getSubjectVar();
    Resource subj = (Resource) getConstantValue(subjectVar);
    Var predicateVar = sp.getPredicateVar();
    URI pred = (URI) getConstantValue(predicateVar);
    Var objectVar = sp.getObjectVar();
    org.openrdf.model.Value obj = getConstantValue(objectVar);
    Resource context = (Resource) getConstantValue(sp.getContextVar());

    /**
     * We put full triple scans before rdf:type because more often than not the triple scan is being joined with something else that is better than asking the
     * full rdf:type of everything.
     */
    double cardinality = 0;
    try {
        cardinality = 2*getTableSize(conf);
    } catch (Exception e1) {
        e1.printStackTrace();
    }
    try {
      if (subj != null) {
        List<org.openrdf.model.Value> values = new ArrayList<org.openrdf.model.Value>();
        CARDINALITY_OF card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECT;
        values.add(subj);

        if (pred != null) {
          values.add(pred);
          card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTPREDICATE;
        } else if (obj != null) {
          values.add(obj);
          card = RdfEvalStatsDAO.CARDINALITY_OF.SUBJECTOBJECT;
        }

        double evalCard = this.getCardinality(conf, card, values, context);
        // the cardinality will be -1 if there was no value found (if
        // the index does not exist)
        if (evalCard >= 0) {
          cardinality = Math.min(cardinality, evalCard);
        } else {
          // TODO change this to agree with prospector
          cardinality = 0;
        }
      } else if (pred != null) {
        List<org.openrdf.model.Value> values = new ArrayList<org.openrdf.model.Value>();
        CARDINALITY_OF card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE;
        values.add(pred);

        if (obj != null) {
          values.add(obj);
          card = RdfEvalStatsDAO.CARDINALITY_OF.PREDICATEOBJECT;
        }

        double evalCard = this.getCardinality(conf, card, values, context);
        if (evalCard >= 0) {
          cardinality = Math.min(cardinality, evalCard);
        } else {
          // TODO change this to agree with prospector
          cardinality = 0;
        }
      } else if (obj != null) {
        List<org.openrdf.model.Value> values = new ArrayList<org.openrdf.model.Value>();
        values.add(obj);
        double evalCard = this.getCardinality(conf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values, context);
        if (evalCard >= 0) {
          cardinality = Math.min(cardinality, evalCard);
        } else {
          // TODO change this to agree with prospector
          cardinality = 0;
        }
      } else {
          cardinality = getTableSize(conf);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    // TODO is this okay?
    return (long) cardinality;
  }

  private org.openrdf.model.Value getConstantValue(Var var) {
    if (var != null)
      return var.getValue();
    else
      return null;
  }

  public double getCardinality(RdfCloudTripleStoreConfiguration conf, CARDINALITY_OF card, List<org.openrdf.model.Value> val) throws RdfDAOException {
    return resd.getCardinality(conf, card, val);
  }

  public double getCardinality(RdfCloudTripleStoreConfiguration conf, CARDINALITY_OF card, List<org.openrdf.model.Value> val, Resource context) throws RdfDAOException {

    return resd.getCardinality(conf, card, val, context);

  }

  public int getTableSize(RdfCloudTripleStoreConfiguration conf) throws TableNotFoundException {

      Authorizations authorizations = getAuths(conf);
    

    if (joinMap.containsKey("subjectpredicateobject" + DELIM + "FullTableCardinality")) {
      FullTableCardinality = joinMap.get("subjectpredicateobject" + DELIM + "FullTableCardinality").intValue();
      return FullTableCardinality;
    }

    if (FullTableCardinality == 0) {
      Scanner joinScanner = connector.createScanner(tableLayoutStrategy.getSelectivity(), authorizations);
      joinScanner.setRange(Range.prefix(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality")));
      Iterator<Map.Entry<Key,Value>> iterator = joinScanner.iterator();
      if (iterator.hasNext()) {
        Map.Entry<Key,Value> entry = iterator.next();
        if (entry.getKey().getColumnFamily().toString().equals("FullTableCardinality")) {
          String Count = entry.getKey().getColumnQualifier().toString();
          FullTableCardinality = Integer.parseInt(Count);
        }
      }
      if (FullTableCardinality == 0) {
        throw new RuntimeException("Table does not contain full cardinality");
      }

    }

    return FullTableCardinality;

  }
  
  
  private Authorizations getAuths(RdfCloudTripleStoreConfiguration conf) {
      String[] auths = conf.getAuths();
      Authorizations authorizations = null;
      if (auths == null || auths.length == 0) {
          authorizations = new Authorizations();
      } else {
          authorizations = new Authorizations(auths);
      }
      
      return authorizations;
  }
  
  
  
  private static class SpExternalCollector extends QueryModelVisitorBase<RuntimeException> {

      private List<QueryModelNode> eSet = Lists.newArrayList();
        
      
      @Override
      public void meetNode(QueryModelNode node) throws RuntimeException {
          if (node instanceof ExternalSet || node instanceof StatementPattern) {
              eSet.add(node);
          }
          super.meetNode(node);
      }

      public List<QueryModelNode> getSpExtTup() {
          return eSet;
      }

  }
  
  
  
  
  

}
