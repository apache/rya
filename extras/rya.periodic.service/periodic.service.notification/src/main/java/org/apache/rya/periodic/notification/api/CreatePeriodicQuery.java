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
package org.apache.rya.periodic.notification.api;

import java.util.Optional;

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryNode;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.periodic.notification.application.PeriodicNotificationApplication;
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
 * <li>
 * <li> prefix function: http://org.apache.rya/function#
 * <li>               "prefix time: http://www.w3.org/2006/time# 
 * <li>               "select (count(?obs) as ?total) where {
 * <li>               "Filter(function:periodic(?time, 1, .25, time:minutes))
 * <li>               "?obs uri:hasTime ?time.
 * <li>               "?obs uri:hasId ?id }
 * <li>
 * 
 * This class is responsible for taking a Periodic Query expressed as a SPARQL query
 * and adding to Fluo and Kafka so that it can be processed by the {@link PeriodicNotificationApplication}.
 */
public class CreatePeriodicQuery {

    private FluoClient fluoClient;
    private PeriodicQueryResultStorage periodicStorage;
    Function funciton;
    PeriodicQueryUtil util;
    
    
    public CreatePeriodicQuery(FluoClient fluoClient, PeriodicQueryResultStorage periodicStorage) {
        this.fluoClient = fluoClient;
        this.periodicStorage = periodicStorage;
    }
    
    /**
     * Creates a Periodic Query by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @return PeriodicNotification that can be used to register register this query with the {@link PeriodicNotificationApplication}.
     */
    public PeriodicNotification createPeriodicQuery(String sparql) {
        try {
            Optional<PeriodicQueryNode> optNode = PeriodicQueryUtil.getPeriodicNode(sparql);
            if(optNode.isPresent()) {
                PeriodicQueryNode periodicNode = optNode.get();
                String pcjId = FluoQueryUtils.createNewPcjId();
               
                //register query with Fluo
                CreateFluoPcj createPcj = new CreateFluoPcj();
                createPcj.createPcj(pcjId, sparql, Sets.newHashSet(ExportStrategy.RYA), fluoClient);
                
                //register query with PeriodicResultStorage table
                periodicStorage.createPeriodicQuery(pcjId, sparql);
                //create notification
                PeriodicNotification notification = PeriodicNotification.builder().id(pcjId).period(periodicNode.getPeriod())
                        .timeUnit(periodicNode.getUnit()).build();
                return notification;
            } else {
                throw new RuntimeException("Invalid PeriodicQuery.  Query must possess a PeriodicQuery Filter.");
            }
        } catch (MalformedQueryException | PeriodicQueryStorageException | UnsupportedQueryException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Creates a Periodic Query by adding the query to Fluo and using the resulting
     * Fluo id to create a {@link PeriodicQueryResultStorage} table.  In addition, this
     * method registers the PeriodicQuery with the PeriodicNotificationApplication to poll
     * the PeriodicQueryResultStorage table at regular intervals and export results to Kafka.
     * The PeriodicNotificationApp queries the result table at a regular interval indicated by the Period of
     * the PeriodicQuery.
     * @param sparql - sparql query registered to Fluo whose results are stored in PeriodicQueryResultStorage table
     * @param PeriodicNotificationClient - registers the PeriodicQuery with the {@link PeriodicNotificationApplication}
     * @return id of the PeriodicQuery and PeriodicQueryResultStorage table (these are the same)
     */
    public String createQueryAndRegisterWithKafka(String sparql, PeriodicNotificationClient periodicClient) {
        PeriodicNotification notification = createPeriodicQuery(sparql);
        periodicClient.addNotification(notification);
        return notification.getId();
    }
    
}
