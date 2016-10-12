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
package org.apache.rya.indexing.pcj.fluo.app.observers;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;

import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.app.IncUpdateDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;

import com.google.common.collect.Maps;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.observer.AbstractObserver;

/**
 * An observer that matches new Triples to the Statement Patterns that are part
 * of any PCJ that is being maintained. If the triple matches a pattern, then
 * the new result is stored as a binding set for the pattern.
 */
public class TripleObserver extends AbstractObserver {

    private static final FluoQueryMetadataDAO QUERY_DAO = new FluoQueryMetadataDAO();
    private static final VisibilityBindingSetStringConverter CONVERTER = new VisibilityBindingSetStringConverter();

    public TripleObserver() {}

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.TRIPLES, NotificationType.STRONG);
    }

    @Override
    public void process(final TransactionBase tx, final Bytes brow, final Column column) {
        //get string representation of triple
        String row = brow.toString();
        final String triple = IncUpdateDAO.getTripleString(brow);
        String visibility = tx.gets(row, FluoQueryColumns.TRIPLES, "");
       
        //get variable metadata for all SP in table
        RowScanner rscanner = tx.scanner().over(Span.prefix(SP_PREFIX)).fetch(FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER).byRow().build();
       

        //see if triple matches conditions of any of the SP

        for (ColumnScanner colScanner : rscanner) {
            final String spID = colScanner.getsRow();

            final StatementPatternMetadata spMetadata = QUERY_DAO.readStatementPatternMetadata(tx, spID);
            final String pattern = spMetadata.getStatementPattern();
            
            for (ColumnValue cv : colScanner) {
                final String varOrders = cv.getsValue();
                final VariableOrder varOrder = new VariableOrder(varOrders);
                final String bindingSetString = getBindingSet(triple, pattern, varOrders);

                //Statement matches to a binding set
                if(bindingSetString.length() != 0) {
                    final VisibilityBindingSet bindingSet = new VisibilityBindingSet(
                        CONVERTER.convert(bindingSetString, varOrder),
                        visibility);
                    final String valueString = CONVERTER.convert(bindingSet, varOrder);
                    tx.set(spID + NODEID_BS_DELIM + bindingSetString, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET, valueString);
                }
			}
		}

        // Once the triple has been handled, it may be deleted.
        tx.delete(row, column);
    }

    /**
     * Determines whether triple matches Statement Pattern ID conditions if
     * so, generates a string representation of a BindingSet whose order
     * is determined by varOrder.
     * @param triple - The triple to consider.
     * @param spID - The statement pattern ID
     * @param varOrder - The variable order
     * @return The string representation of the BindingSet or an empty string,
     * signifying the triple did not match the statement pattern ID.
     */
    private static String getBindingSet(final String triple, final String spID, final String varOrder) {
        final String[] spIdArray = spID.split(DELIM);
        final String[] tripleArray = triple.split(DELIM);
        final String[] varOrderArray = varOrder.split(VAR_DELIM);
        final Map<String,String> varMap = Maps.newHashMap();

        if(spIdArray.length != 3 || tripleArray.length != 3) {
            throw new IllegalArgumentException("Invald number of components");
        }

        for(int i = 0; i < 3; i ++) {

            if(spIdArray[i].startsWith("-const-")) {
                if(!spIdArray[i].substring(7).equals(tripleArray[i])) {
                    return "";
                }
            } else{
                varMap.put(spIdArray[i], tripleArray[i]);
            }

        }

        String bindingSet = "";

        for (final String element : varOrderArray) {
            if(bindingSet.length() == 0) {
                bindingSet = varMap.get(element);
            } else {
                bindingSet = bindingSet + DELIM + varMap.get(element);
            }
        }

        return bindingSet;
    }
}
