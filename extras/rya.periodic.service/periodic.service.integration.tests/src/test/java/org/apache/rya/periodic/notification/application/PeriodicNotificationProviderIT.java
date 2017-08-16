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
package org.apache.rya.periodic.notification.application;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.pcj.fluo.test.base.ModifiedAccumuloExportITBase;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.recovery.PeriodicNotificationProvider;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;

public class PeriodicNotificationProviderIT extends ModifiedAccumuloExportITBase {

    @Test
    public void testProvider() throws MalformedQueryException, InterruptedException {

        final String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n

        final BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        final PeriodicNotificationCoordinatorExecutor coord = new PeriodicNotificationCoordinatorExecutor(2, notifications);
        final PeriodicNotificationProvider provider = new PeriodicNotificationProvider();
        final CreatePcj pcj = new CreatePcj();

        String id = null;
        try(FluoClient fluo = new FluoClientImpl(getFluoConfiguration())) {
            id = pcj.createPcj(sparql, fluo);
            provider.processRegisteredNotifications(coord, fluo.newSnapshot());
        }

        final TimestampedNotification notification = notifications.poll(30, TimeUnit.SECONDS);
        Assert.assertNotNull("timed out before we received a notification", notification);
        Assert.assertEquals(5000, notification.getInitialDelay());
        Assert.assertEquals(15000, notification.getPeriod());
        Assert.assertEquals(TimeUnit.MILLISECONDS, notification.getTimeUnit());
        Assert.assertEquals(id, notification.getId());

    }

}
