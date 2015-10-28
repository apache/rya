package mvm.rya.api.persist.joinselect;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 - 2015 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RdfEvalStatsDAO;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

public interface SelectivityEvalDAO<C extends RdfCloudTripleStoreConfiguration> extends RdfEvalStatsDAO<C> {

  public double getJoinSelect(C conf, TupleExpr te1, TupleExpr te2) throws Exception;

  public long getCardinality(C conf, StatementPattern sp) throws Exception;
  
  public int getTableSize(C conf) throws Exception;

}
