/**
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

import static java.util.Objects.requireNonNull;

/**
 * Utilities that are useful for interacting with {@link Thread}s while testing.
 */
public class ThreadUtil {

    /**
     * Private constructor to prevent instantiation.
     */
    private ThreadUtil() { }

    /**
     * A utility function that returns whether a thread is alive or not after waiting
     * some specified period of time to join it.
     *
     * @param thread - The thread that will be joined. (not null)
     * @param millis - How long to wait to join the thread.
     * @return {@code true} if the thread is still alive, otherwise {@code false}.
     */
    public static boolean stillAlive(final Thread thread, final long millis) {
        requireNonNull(thread);
        try {
            thread.join(millis);
        } catch (final InterruptedException e) { }
        return thread.isAlive();
    }
}