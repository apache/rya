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
package org.apache.rya.periodic.notification.recovery;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;

/**
 * This class is used by the {@link PeriodicNotificationCoordinatorExecutor}
 * to add all existing {@link PeriodicNotification}s stored in Fluo when it is
 * initialized.  This enables the the {@link PeriodicServiceApplication} to be 
 * recovered from failure by restoring it original state.
 *
 */
public class PeriodicNotificationProvider {

    private FluoQueryMetadataDAO dao;
    
    public PeriodicNotificationProvider() {
        this.dao = new FluoQueryMetadataDAO();
    }
    
    /**
     * Retrieve all of the information about Periodic Query results already registered
     * with Fluo.  This is returned in the form of {@link CommandNotification}s that
     * can be registered with the {@link NotificationCoordinatorExecutor}.
     * @param sx - snapshot for reading results from Fluo
     * @return - collection of CommandNotifications that indicate Periodic Query information registered with system
     */
    public Collection<CommandNotification> getNotifications(Snapshot sx) {
        Set<PeriodicQueryMetadata> periodicMetadata = new HashSet<>();
        RowScanner scanner = sx.scanner().fetch(FluoQueryColumns.PERIODIC_QUERY_NODE_ID)
                .over(Span.prefix(IncrementalUpdateConstants.PERIODIC_QUERY_PREFIX)).byRow().build();
        Iterator<ColumnScanner> colScannerIter = scanner.iterator();
        while (colScannerIter.hasNext()) {
            ColumnScanner colScanner = colScannerIter.next();
            Iterator<ColumnValue> values = colScanner.iterator();
            while (values.hasNext()) {
                PeriodicQueryMetadata metadata = dao.readPeriodicQueryMetadata(sx, values.next().getsValue());
                periodicMetadata.add(metadata);
            }
        }
        return getCommandNotifications(sx, periodicMetadata);
    }
    
    /**
     * Registers all of Periodic Query information already contained within Fluo to the 
     * {@link NotificationCoordinatorExecutor}.
     * @param coordinator - coordinator that periodic info will be registered with
     * @param sx - snapshot for reading results from Fluo
     */
    public void processRegisteredNotifications(NotificationCoordinatorExecutor coordinator, Snapshot sx) {
        coordinator.start();
        Collection<CommandNotification> notifications = getNotifications(sx);
        for(CommandNotification notification: notifications) {
            coordinator.processNextCommandNotification(notification);
        }
    }
    
    private Collection<CommandNotification> getCommandNotifications(Snapshot sx, Collection<PeriodicQueryMetadata> metadata) {
        Set<CommandNotification> notifications = new HashSet<>();
        int i = 1;
        for(PeriodicQueryMetadata meta:metadata) {
            //offset initial wait to avoid overloading system
            PeriodicNotification periodic = new PeriodicNotification(getQueryId(meta.getNodeId(), sx), meta.getPeriod(),TimeUnit.MILLISECONDS,i*5000);
            notifications.add(new CommandNotification(Command.ADD, periodic));
            i++;
        }
        return notifications;
    }
    
    private String getQueryId(String periodicNodeId, Snapshot sx) {
        return getQueryIdFromPeriodicId(sx, periodicNodeId);
    }
    
    private String getQueryIdFromPeriodicId(Snapshot sx, String nodeId) {
        NodeType nodeType = NodeType.fromNodeId(nodeId).orNull();
        String id = null;
        switch (nodeType) {
        case FILTER:
            id = getQueryIdFromPeriodicId(sx, sx.get(Bytes.of(nodeId), FluoQueryColumns.FILTER_PARENT_NODE_ID).toString());
            break;
        case PERIODIC_QUERY:
            id = getQueryIdFromPeriodicId(sx, sx.get(Bytes.of(nodeId), FluoQueryColumns.PERIODIC_QUERY_PARENT_NODE_ID).toString());
            break;
        case QUERY:
            id = sx.get(Bytes.of(nodeId), FluoQueryColumns.RYA_PCJ_ID).toString();
            break;
        case AGGREGATION: 
            id = getQueryIdFromPeriodicId(sx, sx.get(Bytes.of(nodeId), FluoQueryColumns.AGGREGATION_PARENT_NODE_ID).toString());
            break;
        case CONSTRUCT:
            id = sx.get(Bytes.of(nodeId), FluoQueryColumns.CONSTRUCT_NODE_ID).toString();
            id = id.split(IncrementalUpdateConstants.CONSTRUCT_PREFIX)[1];
            break;
        default:
            throw new RuntimeException("Invalid NodeType.");
        }
        return id;
    }
    
}
