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
package org.apache.rya.test.accumulo;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.rules.ExternalResource;

public class RyaTestInstanceRule extends ExternalResource {

    private static final AtomicInteger ryaInstanceNameCounter = new AtomicInteger(1);
    private static final AtomicInteger userId = new AtomicInteger(1);

    private final Optional<DoInstall> doInstall;
    private String ryaInstanceName;

    /**
     * Invoked within {@link RyaTestInstanceRule#before()} when provided.
     */
    public static interface DoInstall {

        /**
         * Invoked within {@link RyaTestInstanceRule#before()}.
         *
         * @param ryaInstanceName - The Rya Instance name for the test. (not null)
         * @throws Throwable Anything caused the install to fail.
         */
        public void doInstall(String ryaInstanceName) throws Throwable;
    }

    /**
     * Constructs an instance of {@link RyaTestInstnaceRule} where no extra steps need
     * to be performed within {@link #before()}.
     */
    public RyaTestInstanceRule() {
        this.doInstall = Optional.empty();
    }

    /**
     * Constructs an instance of {@link RyaTestInstnaceRule}.
     *
     * @param doInstall - Invoked within {@link #before()}. (not null)
     */
    public RyaTestInstanceRule(final DoInstall doInstall) {
        this.doInstall = Optional.of(doInstall);
    }

    public String getRyaInstanceName() {
        if (ryaInstanceName == null) {
            throw new IllegalStateException("Cannot get rya instance name outside of a test execution.");
        }
        return ryaInstanceName;
    }

    public String createUniqueUser() {
        final int id = userId.getAndIncrement();
        return "user_" + id;
    }

    @Override
    protected void before() throws Throwable {
        // Get the next Rya instance name.
        ryaInstanceName = "testInstance_" + ryaInstanceNameCounter.getAndIncrement();

        if (doInstall.isPresent()) {
            doInstall.get().doInstall(ryaInstanceName);
        }
    }

    @Override
    protected void after() {
        ryaInstanceName = null;
        // TODO consider teardown of instance (probably requires additional features)
    }
}