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
package org.apache.rya.pcj.fluo.test.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchObserver;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.PeriodicQueryObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;

/**
 * The base Integration Test class used for Fluo applications that export to a Rya PCJ Index.
 */
public class RyaExportITBase extends FluoITBase {


    @Override
    protected void preFluoInitHook() throws Exception {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(BatchObserver.class.getName()));
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));
        observers.add(new ObserverSpecification(AggregationObserver.class.getName()));
        observers.add(new ObserverSpecification(PeriodicQueryObserver.class.getName()));

        // Configure the export observer to export new PCJ results to the mini accumulo cluster.
        final HashMap<String, String> exportParams = new HashMap<>();
        final RyaExportParameters ryaParams = new RyaExportParameters(exportParams);
        ryaParams.setExportToRya(true);
        ryaParams.setRyaInstanceName(getRyaInstanceName());
        ryaParams.setAccumuloInstanceName(super.getMiniAccumuloCluster().getInstanceName());
        ryaParams.setZookeeperServers(super.getMiniAccumuloCluster().getZooKeepers());
        ryaParams.setExporterUsername(getUsername());
        ryaParams.setExporterPassword(getPassword());

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }

}