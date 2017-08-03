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
package org.apache.rya.shell.util;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.beust.jcommander.internal.Lists;

/**
 * Tests an instance of {@link InstanceNamesFormatter}.
 */
public class InstanceNamesFormatterTest {

    @Test
    public void format_withConnectedName() {
        final List<String> instanceNames = Lists.newArrayList("a", "b", "c", "d");

        final String formatted = new InstanceNamesFormatter().format(instanceNames, "c");

        final String expected =
                "Rya instance names:\n" +
                "   a\n" +
                "   b\n" +
                " * c\n" +
                "   d\n";

        assertEquals(expected, formatted);
    }

    @Test
    public void format_connectedNameNotInList() {
        final List<String> instanceNames = Lists.newArrayList("a", "b", "c", "d");

        final String formatted = new InstanceNamesFormatter().format(instanceNames, "not_in_list");

        final String expected =
                "Rya instance names:\n" +
                "   a\n" +
                "   b\n" +
                "   c\n" +
                "   d\n";

        assertEquals(expected, formatted);
    }

    @Test
    public void format() {
        final List<String> instanceNames = Lists.newArrayList("a", "b", "c", "d");

        final String formatted = new InstanceNamesFormatter().format(instanceNames);

        final String expected =
                "Rya instance names:\n" +
                "   a\n" +
                "   b\n" +
                "   c\n" +
                "   d\n";

        assertEquals(expected, formatted);
    }
}