/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.api.client;

/**
 * This class creates new PeriodicPCJ for a given Rya instance.  
 */
public interface CreatePeriodicPCJ {

    /**
     * Creates a new PeriodicPCJ for a given Rya instance. The provided periodicTopic and bootStrapServers are used for
     * registering new PeriodiNotifications with the underlying notification registration service. Typically, the
     * bootStrapServers are the IP for the KafkaBrokers.
     * 
     * @param instanceName - Rya instance to connect to
     * @param sparql - SPARQL query registered with the Periodic Service
     * @param periodicTopic - Kafka topic that new PeriodicNotifications are exported to for registration with the
     *            PeriodicService
     * @param bootStrapServers - Connection string for Kafka brokers
     * @return Fluo Query Id of the registered Periodic Query
     */
    public String createPeriodicPCJ(String instanceName, String sparql, String periodicTopic, String bootStrapServers) throws RyaClientException;
    
}
