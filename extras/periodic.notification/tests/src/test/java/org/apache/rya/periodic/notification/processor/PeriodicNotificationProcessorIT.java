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
package org.apache.rya.periodic.notification.processor;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.junit.Assert;
import org.junit.Test;

public class PeriodicNotificationProcessorIT extends AccumuloExportITBase {

    private static final ValueFactory vf = SimpleValueFactory.getInstance();
    private static final String RYA_INSTANCE_NAME = "rya_";
    
    @Test
    public void periodicProcessorTest() throws Exception {
        
        String id = UUID.randomUUID().toString().replace("-", "");
        BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        BlockingQueue<NodeBin> bins = new LinkedBlockingQueue<>();
        BlockingQueue<BindingSetRecord> bindingSets = new LinkedBlockingQueue<>();
        
        TimestampedNotification ts1 = new TimestampedNotification(
                PeriodicNotification.builder().id(id).initialDelay(0).period(2000).timeUnit(TimeUnit.SECONDS).build());  
        long binId1 = (ts1.getTimestamp().getTime()/ts1.getPeriod())*ts1.getPeriod();
        
        Thread.sleep(2000);
        
        TimestampedNotification ts2 = new TimestampedNotification(
                PeriodicNotification.builder().id(id).initialDelay(0).period(2000).timeUnit(TimeUnit.SECONDS).build());  
        long binId2 = (ts2.getTimestamp().getTime()/ts2.getPeriod())*ts2.getPeriod();
        
        Set<NodeBin> expectedBins = new HashSet<>();
        expectedBins.add(new NodeBin(id, binId1));
        expectedBins.add(new NodeBin(id, binId2));
        
        Set<BindingSet> expected = new HashSet<>();
        Set<VisibilityBindingSet> storageResults = new HashSet<>();
        
        QueryBindingSet bs1 = new QueryBindingSet();
        bs1.addBinding("periodicBinId", vf.createLiteral(binId1));
        bs1.addBinding("id", vf.createLiteral(1));
        expected.add(bs1);
        storageResults.add(new VisibilityBindingSet(bs1));
        
        QueryBindingSet bs2 = new QueryBindingSet();
        bs2.addBinding("periodicBinId", vf.createLiteral(binId1));
        bs2.addBinding("id", vf.createLiteral(2));
        expected.add(bs2);
        storageResults.add(new VisibilityBindingSet(bs2));
        
        QueryBindingSet bs3 = new QueryBindingSet();
        bs3.addBinding("periodicBinId", vf.createLiteral(binId2));
        bs3.addBinding("id", vf.createLiteral(3));
        expected.add(bs3);
        storageResults.add(new VisibilityBindingSet(bs3));
        
        QueryBindingSet bs4 = new QueryBindingSet();
        bs4.addBinding("periodicBinId", vf.createLiteral(binId2));
        bs4.addBinding("id", vf.createLiteral(4));
        expected.add(bs4);
        storageResults.add(new VisibilityBindingSet(bs4));
        
        PeriodicQueryResultStorage periodicStorage = new AccumuloPeriodicQueryResultStorage(super.getAccumuloConnector(),
                RYA_INSTANCE_NAME);
        periodicStorage.createPeriodicQuery(id, "select ?id where {?obs <urn:hasId> ?id.}", new VariableOrder("periodicBinId", "id"));
        periodicStorage.addPeriodicQueryResults(id, storageResults);

        NotificationProcessorExecutor processor = new NotificationProcessorExecutor(periodicStorage, notifications, bins, bindingSets, 1);
        processor.start();
        
        notifications.add(ts1);
        notifications.add(ts2);

        Thread.sleep(5000);
        
        Assert.assertEquals(expectedBins.size(), bins.size());
        Assert.assertEquals(true, bins.containsAll(expectedBins));
        
        Set<BindingSet> actual = new HashSet<>();
        bindingSets.forEach(x -> actual.add(x.getBindingSet()));
        Assert.assertEquals(expected, actual);
        
        processor.stop();
    }
    
}
