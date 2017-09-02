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
package org.apache.rya.periodic.notification.pruner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoClient;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.apache.rya.periodic.notification.api.NodeBin;

import com.google.common.base.Preconditions;

/**
 * Executor service that runs {@link PeriodicQueryPruner}s with added functionality
 * for starting, stopping, and determining if the query pruners are running.
 */
public class PeriodicQueryPrunerExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(PeriodicQueryPrunerExecutor.class);
    private FluoClient client;
    private int numThreads;
    private ExecutorService executor;
    private BlockingQueue<NodeBin> bins;
    private PeriodicQueryResultStorage periodicStorage;
    private List<PeriodicQueryPruner> pruners;
    private boolean running = false;

    public PeriodicQueryPrunerExecutor(PeriodicQueryResultStorage periodicStorage, FluoClient client, int numThreads,
            BlockingQueue<NodeBin> bins) {
        Preconditions.checkArgument(numThreads > 0);
        this.periodicStorage = periodicStorage;
        this.numThreads = numThreads;
        executor = Executors.newFixedThreadPool(numThreads);
        this.bins = bins;
        this.client = client;
        this.pruners = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            AccumuloBinPruner accPruner = new AccumuloBinPruner(periodicStorage);
            FluoBinPruner fluoPruner = new FluoBinPruner(client);

            for (int threadNumber = 0; threadNumber < numThreads; threadNumber++) {
                PeriodicQueryPruner pruner = new PeriodicQueryPruner(fluoPruner, accPruner, client, bins, threadNumber);
                pruners.add(pruner);
                executor.submit(pruner);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (pruners != null && pruners.size() > 0) {
            pruners.forEach(x -> x.shutdown());
        }
        if(client != null) {
            client.close();
        }
        if (executor != null) {
            executor.shutdown();
            running = false;
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
