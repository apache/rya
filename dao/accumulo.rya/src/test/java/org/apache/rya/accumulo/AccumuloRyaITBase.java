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
package org.apache.rya.accumulo;

import org.junit.Rule;

/**
 * Contains boilerplate code for spinning up a Mini Accumulo Cluster and initializing
 * some of the Rya stuff. We can not actually initialize an instance of Rya here
 * because Sail is not available to us.
 */
public class AccumuloRyaITBase {

    @Rule
    public RyaTestInstanceRule testInstance = new RyaTestInstanceRule(true);

    /**
     * @return The {@link MiniAccumuloClusterInstance} used by the tests.
     */
    public MiniAccumuloClusterInstance getClusterInstance() {
        return MiniAccumuloSingleton.getInstance();
    }

    /**
     * @return The name of the Rya instance that is being used for the current test.
     */
    public String getRyaInstanceName() {
        return testInstance.getRyaInstanceName();
    }
}