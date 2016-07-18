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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the methods of {@link PcjTableNameFactory}.
 */
public class PcjTableNameFactoryTest {

    @Test
    public void makeTableName() {
        final String ryaInstance = "testInstance_";
        final String pcjId = "2dda1b099d264f16b1da8f9409c104d3";

        // Create the Accumulo PCJ table name.
        final PcjTableNameFactory factory = new PcjTableNameFactory();
        final String tableName = factory.makeTableName(ryaInstance, pcjId);

        // Ensure the table name matches the expected name.
        assertEquals("testInstance_INDEX_2dda1b099d264f16b1da8f9409c104d3", tableName);
    }

    @Test
    public void getPcjId() {
        final String pcjTableName = "testInstance_INDEX_2dda1b099d264f16b1da8f9409c104d3";

        // Get the PCJ ID from the table name.
        final PcjTableNameFactory factory = new PcjTableNameFactory();
        final String pcjId = factory.getPcjId(pcjTableName);

        // Ensure the pcjId matches the expected id.
        assertEquals("2dda1b099d264f16b1da8f9409c104d3", pcjId);
    }
}