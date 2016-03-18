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
package org.apache.rya.indexing.pcj.fluo.client;

import static org.junit.Assert.assertEquals;

import org.apache.rya.indexing.pcj.fluo.client.util.Report;
import org.apache.rya.indexing.pcj.fluo.client.util.Report.ReportItem;
import org.junit.Test;

/**
 * Tests the methods of {@link Report}.
 */
public class ReportTests {

    @Test
    public void singleLineValues() {
        final Report.Builder builder = Report.builder();
        builder.appendItem(new ReportItem("Title 1", new String[]{"Short value."}));
        builder.appendItem(new ReportItem("Title 2", new String[]{"This is the longest values that appears in the report."}));
        builder.appendItem(new ReportItem("This is a long title", new String[]{"Short value."}));
        final Report report = builder.build();

        final String expected =
                "---------------------------------------------------------------------------------\n" +
                "| Title 1              | Short value.                                           |\n" +
                "| Title 2              | This is the longest values that appears in the report. |\n" +
                "| This is a long title | Short value.                                           |\n" +
                "---------------------------------------------------------------------------------\n";

        assertEquals(expected, report.toString());
    }

    @Test
    public void emptyValues() {
        final Report.Builder builder = Report.builder();
        builder.appendItem(new ReportItem("No Value Here", new String[]{}));
        builder.appendItem(new ReportItem("Value Here", new String[]{"This one has a value."}));
        final Report report = builder.build();

        final String expected =
                "-----------------------------------------\n" +
                "| No Value Here |                       |\n" +
                "| Value Here    | This one has a value. |\n" +
                "-----------------------------------------\n";
        assertEquals(expected, report.toString());
    }

    @Test
    public void multiLineValues() {
        final Report.Builder builder = Report.builder();
        builder.appendItem(new ReportItem("Title 1", new String[]{"Value 1"}));
        builder.appendItem(new ReportItem("Multiple Lines", new String[]{"This is the first line.", "This is the second line."}));
        builder.appendItem(new ReportItem("Title 2", new String[]{"Value 2"}));
        final Report report = builder.build();

        final String expected =
                "---------------------------------------------\n" +
                "| Title 1        | Value 1                  |\n" +
                "| Multiple Lines | This is the first line.  |\n" +
                "|                | This is the second line. |\n" +
                "| Title 2        | Value 2                  |\n" +
                "---------------------------------------------\n";
        assertEquals(expected, report.toString());
    }
}