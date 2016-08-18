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
package org.apache.rya.benchmark.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.rya.benchmark.query.Parameters.NumReadsRuns;
import org.apache.rya.benchmark.query.Parameters.Queries;
import org.apache.rya.benchmark.query.Rya.Accumulo;
import org.apache.rya.benchmark.query.Rya.SecondaryIndexing;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/**
 * Tests the methods of {@link BenchmarkQueriesReader}.
 */
public class QueriesBenchmarkConfReaderIT {

    @Test
    public void load() throws JAXBException, SAXException {
        // Unmarshal some XML.
        final String xml =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<QueriesBenchmarkConf>\n" +
                "    <Rya>\n" +
                "        <ryaInstanceName>test_</ryaInstanceName>\n" +
                "        <accumulo>\n" +
                "            <username>test</username>\n" +
                "            <password>t3stP@ssw0rd</password>\n" +
                "            <zookeepers>zoo-server-1,zoo-server-2</zookeepers>\n" +
                "            <instanceName>testInstance</instanceName>\n" +
                "        </accumulo>\n" +
                "        <secondaryIndexing>\n" +
                "            <usePCJ>true</usePCJ>\n" +
                "        </secondaryIndexing>\n" +
                "    </Rya>\n" +
                "    <Parameters>" +
                "        <NumReadsRuns>" +
                "            <NumReads>1</NumReads>" +
                "            <NumReads>10</NumReads>" +
                "            <NumReads>100</NumReads>" +
                "            <NumReads>ALL</NumReads>" +
                "        </NumReadsRuns>" +
                "        <Queries>\n" +
                "            <SPARQL><![CDATA[SELECT ?a WHERE { ?a <http://likes> <urn:icecream> . }]]></SPARQL>\n" +
                "            <SPARQL><![CDATA[SELECT ?a ?b WHERE { ?a <http://knows> ?b . }]]></SPARQL>\n" +
                "        </Queries>\n" +
                "    </Parameters>" +
                "</QueriesBenchmarkConf>";

        final InputStream xmlStream = new ByteArrayInputStream( xml.getBytes(Charsets.UTF_8) );
        final QueriesBenchmarkConf benchmarkConf = new QueriesBenchmarkConfReader().load( xmlStream );

        // Ensure it was unmarshalled correctly.
        final Rya rya = benchmarkConf.getRya();
        assertEquals("test_", rya.getRyaInstanceName());

        final Accumulo accumulo = rya.getAccumulo();
        assertEquals("test", accumulo.getUsername());
        assertEquals("t3stP@ssw0rd", accumulo.getPassword());
        assertEquals("zoo-server-1,zoo-server-2", accumulo.getZookeepers());
        assertEquals("testInstance", accumulo.getInstanceName());

        final SecondaryIndexing secondaryIndexing = rya.getSecondaryIndexing();
        assertTrue(secondaryIndexing.isUsePCJ());


        final Parameters parameters = benchmarkConf.getParameters();
        final List<String> expectedNumReads = Lists.newArrayList("1", "10", "100", "ALL");
        final NumReadsRuns NumReads = parameters.getNumReadsRuns();
        assertEquals(expectedNumReads, NumReads.getNumReads());

        final List<String> expectedQueries = Lists.newArrayList(
                "SELECT ?a WHERE { ?a <http://likes> <urn:icecream> . }",
                "SELECT ?a ?b WHERE { ?a <http://knows> ?b . }");
        final Queries queries = parameters.getQueries();
        assertEquals(expectedQueries, queries.getSPARQL());
    }
}