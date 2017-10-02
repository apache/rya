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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Set;

import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;

import com.google.common.collect.Sets;

public class PeriodicBindingSetExporter implements IncrementalBindingSetExporter {

    private PeriodicQueryResultStorage periodicStorage;
    
    /**
     * Constructs an instance of {@link PeriodicBindingSetExporter}.
     *
     * @param pcjStorage - The PCJ storage the new results will be exported to. (not null)
     */
    public PeriodicBindingSetExporter(PeriodicQueryResultStorage periodicStorage) {
        this.periodicStorage = checkNotNull(periodicStorage);
    }
    
    @Override
    public Set<QueryType> getQueryTypes() {
        return Sets.newHashSet(QueryType.PERIODIC);
    }

    @Override
    public ExportStrategy getExportStrategy() {
        return ExportStrategy.PERIODIC;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void export(String queryId, VisibilityBindingSet result) throws ResultExportException {
        try {
            periodicStorage.addPeriodicQueryResults(queryId, Collections.singleton(result));
        } catch (PeriodicQueryStorageException e) {
            throw new ResultExportException("Could not successfully export the BindingSet: " + result, e);
        }
    }

}
