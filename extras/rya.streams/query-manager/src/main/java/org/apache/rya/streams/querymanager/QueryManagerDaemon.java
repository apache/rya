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
package org.apache.rya.streams.querymanager;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

@DefaultAnnotation(NonNull.class)
public class QueryManagerDaemon implements Daemon {
    @Override
    public void init(final DaemonContext context) throws DaemonInitException, Exception {
        System.out.println("Initializing Query Manager Daemon.");
    }

    @Override
    public void start() throws Exception {
        System.out.println("Starting Query Manager Daemon.");
    }

    @Override
    public void stop() throws Exception {
        System.out.println("Stopping Query Manager Daemon.");
    }

    @Override
    public void destroy() {
        System.out.println("Query Manager Daemon Destroyed.");
    }
}