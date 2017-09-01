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
package org.apache.rya.indexing.pcj.fluo.api;

import java.util.Optional;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryNode;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.evaluation.function.Function;

import com.google.common.collect.Sets;


/**
 * Object that creates a Periodic Query.  A Periodic Query is any query
 * requesting periodic updates about events that occurred within a given
 * window of time of this instant. This is also known as a rolling window
 * query.  Period Queries can be expressed using SPARQL by including the
 * {@link Function} indicated by the URI {@link PeriodicQueryUtil#PeriodicQueryURI}
 * in the query.  The user must provide this Function with the following arguments:
 * the temporal variable in the query that will be filtered on, the window of time
 * that events must occur within, the period at which the user wants to receive updates,
 * and the time unit.  The following query requests all observations that occurred
 * within the last minute and requests updates every 15 seconds.  It also performs
 * a count on those observations.
 * <p>
 * <pre>
 *  prefix function: http://org.apache.rya/function#
 *                "prefix time: http://www.w3.org/2006/time# 
 *                "select (count(?obs) as ?total) where {
 *                "Filter(function:periodic(?time, 1, .25, time:minutes))
 *                "?obs uri:hasTime ?time.
 *                "?obs uri:hasId ?id }
 * </pre>
 * <p>
 * This class is responsible for taking a Periodic Query expressed as a SPARQL query
 * and adding to Fluo and Kafka so that it can be processed by the {@link PeriodicNotificationApplication}.
 */
public class CreatePeriodicQuery {

    private FluoClient fluoClient;
    private PeriodicQueryResultStorage periodicStorage;
    
    
    /**
     * Constructs an instance of CreatePeriodicQuery for creating periodic queries.  An instance
     * of CreatePeriodicQuery that is created using this constructor will not publish new PeriodicNotifications
     * to Kafka.
     * 
     * @param fluoClient - Fluo client for interacting with Fluo
     * @param periodicStorage - PeriodicQueryResultStorage storing periodic query results
     */
    public CreatePeriodicQuery(FluoClient fluoClient, PeriodicQueryResultStorage periodicStorage) {
        this.fluoClient = fluoClient;
        this.periodicStorage = periodicStorage;
    }
    
    
    /**
     * Creates a Periodic Query by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.
     * 
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @return FluoQuery indicating the metadata of the registered SPARQL query
     */
    public FluoQuery createPeriodicQuery(String sparql) throws PeriodicQueryCreationException {
        try {
            Optional<PeriodicQueryNode> optNode = PeriodicQueryUtil.getPeriodicNode(sparql);
            if(optNode.isPresent()) {
                String pcjId = FluoQueryUtils.createNewPcjId();
               
                //register query with Fluo
                CreateFluoPcj createPcj = new CreateFluoPcj();
                FluoQuery fluoQuery = createPcj.createPcj(pcjId, sparql, Sets.newHashSet(ExportStrategy.PERIODIC), fluoClient);
                
                //register query with PeriodicResultStorage table
                periodicStorage.createPeriodicQuery(pcjId, sparql);
                
                return fluoQuery;
            } else {
                throw new RuntimeException("Invalid PeriodicQuery.  Query must possess a PeriodicQuery Filter.");
            }
        } catch (MalformedQueryException | PeriodicQueryStorageException | UnsupportedQueryException e) {
            throw new PeriodicQueryCreationException(e);
        }
    }
    
    
    /**
     * Creates a Periodic Query by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.  Additionally,
     * the associated PeriodicNotification is registered with the Periodic Query Service.
     * 
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @param notificationClient - {@link PeriodicNotificationClient} for registering new PeriodicNotifications
     * @return FluoQuery indicating the metadata of the registered SPARQL query
     */
    public FluoQuery createPeriodicQuery(String sparql, PeriodicNotificationClient notificationClient) throws PeriodicQueryCreationException {
        try {
            Optional<PeriodicQueryNode> optNode = PeriodicQueryUtil.getPeriodicNode(sparql);
            if(optNode.isPresent()) {
                PeriodicQueryNode periodicNode = optNode.get();
                String pcjId = FluoQueryUtils.createNewPcjId();
               
                //register query with Fluo
                CreateFluoPcj createPcj = new CreateFluoPcj();
                FluoQuery fluoQuery = createPcj.createPcj(pcjId, sparql, Sets.newHashSet(ExportStrategy.PERIODIC), fluoClient);
                
                //register query with PeriodicResultStorage table
                periodicStorage.createPeriodicQuery(pcjId, sparql);
                //create notification
                PeriodicNotification notification = PeriodicNotification.builder().id(pcjId).period(periodicNode.getPeriod())
                        .timeUnit(periodicNode.getUnit()).build();
                //register notification with periodic notification app
                notificationClient.addNotification(notification);
                
                return fluoQuery;
            } else {
                throw new RuntimeException("Invalid PeriodicQuery.  Query must possess a PeriodicQuery Filter.");
            }
        } catch (MalformedQueryException | PeriodicQueryStorageException | UnsupportedQueryException e) {
            throw new PeriodicQueryCreationException(e);
        }
    }
    
    
    /**
     * Creates a Periodic Query by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @param notificationClient - {@link PeriodicNotificationClient} for registering new PeriodicNotifications
     * @param conn - Accumulo connector for connecting to the Rya instance
     * @param ryaInstance - name of the Accumulo back Rya instance
     * @return FluoQuery indicating the metadata of the registered SPARQL query
     */
    public FluoQuery withRyaIntegration(String sparql, PeriodicNotificationClient notificationClient, Connector conn, String ryaInstance)
            throws PeriodicQueryCreationException {
        try {
            Optional<PeriodicQueryNode> optNode = PeriodicQueryUtil.getPeriodicNode(sparql);
            if (optNode.isPresent()) {
                PeriodicQueryNode periodicNode = optNode.get();
                String pcjId = FluoQueryUtils.createNewPcjId();

                // register query with Fluo
                CreateFluoPcj createPcj = new CreateFluoPcj();
                FluoQuery fluoQuery = createPcj.withRyaIntegration(pcjId, sparql, Sets.newHashSet(ExportStrategy.PERIODIC),
                        fluoClient, conn, ryaInstance);

                // register query with PeriodicResultStorage table
                periodicStorage.createPeriodicQuery(pcjId, sparql);
                // create notification
                PeriodicNotification notification = PeriodicNotification.builder().id(pcjId).period(periodicNode.getPeriod())
                        .timeUnit(periodicNode.getUnit()).build();
                // register notification with periodic notification app
                notificationClient.addNotification(notification);

                return fluoQuery;
            } else {
                throw new RuntimeException("Invalid PeriodicQuery.  Query must possess a PeriodicQuery Filter.");
            }
        } catch (Exception e) {
            throw new PeriodicQueryCreationException(e);
        }
    }
    
    /**
     * This Exception gets thrown whenever there is an issue creating a PeriodicQuery.
     *
     */
    public static class PeriodicQueryCreationException extends Exception {

        private static final long serialVersionUID = 1L;
        
        public PeriodicQueryCreationException(Exception e) {
            super(e);
        }
        
        public PeriodicQueryCreationException(String message, Exception e) {
            super(message, e);
        }
        
        public PeriodicQueryCreationException(String message) {
            super(message);
        }
        
    }
    
}
