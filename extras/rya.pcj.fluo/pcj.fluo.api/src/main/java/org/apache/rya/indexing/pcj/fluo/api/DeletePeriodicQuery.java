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

import org.apache.fluo.api.client.FluoClient;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.BasicNotification;

import com.google.common.base.Preconditions;

public class DeletePeriodicQuery {

    private FluoClient fluo;
    private PeriodicQueryResultStorage periodicStorage;
    
    public DeletePeriodicQuery(FluoClient fluo, PeriodicQueryResultStorage periodicStorage) {
        this.fluo = fluo;
        this.periodicStorage = periodicStorage;
    }
    
    /**
     * Deletes the Periodic Query with the indicated pcjId from Fluo and {@link PeriodicQueryResultStorage}.
     * @param pcjId - Id of the Periodic Query to be deleted
     */
    public void deletePeriodicQuery(String pcjId) throws QueryDeletionException {
        
        Preconditions.checkNotNull(pcjId);
        
        DeleteFluoPcj deletePcj = new DeleteFluoPcj(1000);
        try {
            deletePcj.deletePcj(fluo, pcjId);
            periodicStorage.deletePeriodicQuery(pcjId);
        } catch (UnsupportedQueryException | PeriodicQueryStorageException e) {
            throw new QueryDeletionException(String.format("Unable to delete the Periodic Query with Id: %s", pcjId), e);
        }
        
    }
    
    /**
     * Deletes the Periodic Query with the indicated pcjId from Fluo and {@link PeriodicQueryResultStorage}. In
     * addition, this method also informs the Periodic Notification Service to stop generating PeriodicNotifications
     * associated with the Periodic Query.
     * 
     * @param queryId - Id of the Periodic Query to be deleted
     * @param periodicClient - Client used to inform the Periodic Notification Service to stop generating notifications
     * @throws QueryDeletionException
     */
    public void deletePeriodicQuery(String pcjId, PeriodicNotificationClient periodicClient) throws QueryDeletionException {
        
        Preconditions.checkNotNull(periodicClient);
        
        deletePeriodicQuery(pcjId);
        periodicClient.deleteNotification(new BasicNotification(pcjId));
    }
    
    /**
     *  This Exception is thrown when a problem is encountered while deleting a
     *  query from the Fluo Application or the underlying storage layer.
     */
    public static class QueryDeletionException extends Exception {
        
        private static final long serialVersionUID = 1L;

        public QueryDeletionException(String message) {
            super(message);
        }
        
        public QueryDeletionException(String message, Exception e) {
            super(message, e);
        }
    }
    
}
