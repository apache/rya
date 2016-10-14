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



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class QueryJoinSelectOptimizer implements QueryOptimizer {

  private final EvaluationStatistics statistics;
  private final SelectivityEvalDAO eval;
  private final RdfCloudTripleStoreConfiguration config;

  public QueryJoinSelectOptimizer(EvaluationStatistics statistics, SelectivityEvalDAO eval) {
    System.out.println("Entering join optimizer!");
    this.statistics = statistics;
    this.eval = eval;
    this.config = eval.getConf();
  }

  /**
   * Applies generally applicable optimizations: path expressions are sorted from more to less specific.
   *
   * @param tupleExpr
   */
  public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
    tupleExpr.visit(new JoinVisitor());
  }

  protected class JoinVisitor extends QueryModelVisitorBase<RuntimeException> {

    @Override
    public void meet(Join node) {

      try {
        if (node.getLeftArg() instanceof FixedStatementPattern && node.getRightArg() instanceof DoNotExpandSP) {
          return;
        }

        TupleExpr partialQuery = null;
        List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<TupleExpr>());
        Map<TupleExpr,Double> cardinalityMap = new HashMap<TupleExpr,Double>();

        for (TupleExpr tupleExpr : joinArgs) {
          double cardinality = statistics.getCardinality(tupleExpr);
          cardinalityMap.put(tupleExpr, cardinality);

        }

        while (!joinArgs.isEmpty()) {
          TePairCost tpc = getBestTupleJoin(partialQuery, joinArgs);
          List<TupleExpr> tePair = tpc.getTePair();
          if (partialQuery == null) {
            if (tePair.size() != 2) {
              throw new IllegalStateException();
            }
            if (!(tePair.get(0) instanceof Join)) {
              tePair.get(0).visit(this);
            }
            if (!(tePair.get(1) instanceof Join)) {
              tePair.get(1).visit(this);
            }
            if (tePair.get(1) instanceof Join) {
              partialQuery = new Join(tePair.get(0), ((Join) tePair.get(1)).getLeftArg());
              partialQuery = new Join(partialQuery, ((Join) tePair.get(1)).getRightArg());
              joinArgs.remove(tePair.get(0));
              joinArgs.remove(tePair.get(1));
            } else {
              partialQuery = new Join(tePair.get(0), tePair.get(1));
              joinArgs.remove(tePair.get(0));
              joinArgs.remove(tePair.get(1));
            }
          } else {
            if (tePair.size() != 1) {
              throw new IllegalStateException();
            }
            if (!(tePair.get(0) instanceof Join)) {
              tePair.get(0).visit(this);
            }

            if (tePair.get(0) instanceof Join) {
              partialQuery = new Join(partialQuery, ((Join) tePair.get(0)).getLeftArg());
              partialQuery = new Join(partialQuery, ((Join) tePair.get(0)).getRightArg());
              joinArgs.remove(tePair.get(0));

            } else {
              partialQuery = new Join(partialQuery, tePair.get(0));
              joinArgs.remove(tePair.get(0));
            }
          }

        }

        // Replace old join hierarchy
        node.replaceWith(partialQuery);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
      if (tupleExpr instanceof Join) {
        if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern) && !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
          Join join = (Join) tupleExpr;
          getJoinArgs(join.getLeftArg(), joinArgs);
          getJoinArgs(join.getRightArg(), joinArgs);
        } else {
          joinArgs.add(tupleExpr);
        }
      } else {
        joinArgs.add(tupleExpr);
      }

      return joinArgs;
    }

    public TePairCost getBestTupleJoin(TupleExpr partialQuery, List<TupleExpr> teList) throws Exception {

      double tempCost = 0;
      double bestCost = Double.MAX_VALUE;
      List<TupleExpr> bestJoinNodes = new ArrayList<TupleExpr>();

      if (partialQuery == null) {

        double jSelect = 0;
        double card1 = 0;
        double card2 = 0;
        TupleExpr teMin1 = null;
        TupleExpr teMin2 = null;
        double bestCard1 = 0;
        double bestCard2 = 0;

        for (int i = 0; i < teList.size(); i++) {
          for (int j = i + 1; j < teList.size(); j++) {
            jSelect = eval.getJoinSelect(config, teList.get(i), teList.get(j));
            card1 = statistics.getCardinality(teList.get(i));
            card2 = statistics.getCardinality(teList.get(j));
            tempCost = card1 + card2 + card1 * card2 * jSelect;
//             System.out.println("Optimizer: TempCost is " + tempCost + " cards are " + card1 + ", " + card2 + ", selectivity is "
//             + jSelect + ", and nodes are "
//             + teList.get(i) + " and " + teList.get(j));

            // TODO this generates a nullpointer exception if tempCost = Double.Max
            if (bestCost > tempCost) {

              teMin1 = teList.get(i);
              teMin2 = teList.get(j);
              bestCard1 = card1;
              bestCard2 = card2;
              bestCost = tempCost;

              if (bestCost == 0) {
                bestJoinNodes.add(teMin1);
                bestJoinNodes.add(teMin2);
                return new TePairCost(0.0, bestJoinNodes);
              }
            }
          }
        }

        if (bestCard1 < bestCard2) {

          bestJoinNodes.add(teMin1);
          bestJoinNodes.add(teMin2);

        } else {
          bestJoinNodes.add(teMin2);
          bestJoinNodes.add(teMin1);
        }
        //System.out.println("Optimizer: Card1 is " + card1 + ", card2 is " + card2 + ", selectivity is " + jSelect + ", and best cost is" + bestCost);
        return new TePairCost(bestCost, bestJoinNodes);

      } else {
        double card1 = statistics.getCardinality(partialQuery);
        TupleExpr bestTe = null;
        double card2 = 0;
        double select = 0;

        for (TupleExpr te : teList) {
          select = eval.getJoinSelect(config, partialQuery, te);
          card2 = statistics.getCardinality(te);
          tempCost = card1 + card2 + card1 * card2 * select;
//          System.out.println("Optimizer: TempCost is " + tempCost + " cards are " + card1 + ", " + card2 + ", selectivity is "
//                  + select + ", and nodes are "
//                  + partialQuery + " and " + te);


          if (bestCost > tempCost) {
            bestTe = te;
            bestCost = tempCost;
          }

        }
        List<TupleExpr> teList2 = new ArrayList<TupleExpr>();
        teList2.add(bestTe);
        //System.out.println("Optimizer: Card1 is " + card1 + ", card2 is " + card2 + ", selectivity is " + select + ", and best cost is" + bestCost);
        return new TePairCost(bestCost, teList2);
      }

    }

    // **************************************************************************************
    public class TePairCost {

      private double cost;
      private List<TupleExpr> tePair;

      public TePairCost(double cost, List<TupleExpr> tePair) {
        this.cost = cost;
        this.tePair = tePair;

      }

      public double getCost() {
        return cost;
      }

      public List<TupleExpr> getTePair() {
        return tePair;
      }

    }

  }
}
