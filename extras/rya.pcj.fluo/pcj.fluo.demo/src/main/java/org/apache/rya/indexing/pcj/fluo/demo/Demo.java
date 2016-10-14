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
package org.apache.rya.indexing.pcj.fluo.demo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.openrdf.repository.RepositoryConnection;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * Represents a demonstration that uses Rya and Fluo on top of Accumulo.
 */
public interface Demo {

    /**
     * Run the demo.
     */
    public void execute(
            MiniAccumuloCluster accumulo,
            Connector accumuloConn,
            String ryaTablePrefix,
            RyaSailRepository ryaRepo,
            RepositoryConnection ryaConn,
            MiniFluo fluo,
            FluoClient fluoClient) throws DemoExecutionException;

    /**
     * A {@link Demo}'s execution could not be completed because of a non-recoverable
     * problem while running.
     */
    public static class DemoExecutionException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs an instance of {@link }.
         *
         * @param message - Describes why this is being thrown.
         * @param cause - The exception that caused this one to be thrown.
         */
        public DemoExecutionException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}