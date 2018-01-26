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
package org.apache.rya.streams.querymanager.xml;

import static org.junit.Assert.assertNotNull;

import java.io.StringReader;

import javax.xml.bind.UnmarshalException;

import org.junit.Test;

/**
 * Unit tests the methods of {@link QueryManagerConfigUnmarshaller}.
 */
public class QueryManagerConfigMarshallerTest {

    @Test
    public void unmarshal_validXml() throws Exception {
        final String xml =
                "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<queryManagerConfig>\n" +
                "    <queryChangeLogSource>\n" +
                "        <kafka>\n" +
                "            <hostname>localhost</hostname>\n" +
                "            <port>6</port>\n" +
                "        </kafka>\n" +
                "    </queryChangeLogSource>\n" +
                "    <performanceTunning>\n" +
                "        <queryChanngeLogDiscoveryPeriod>\n" +
                "            <value>1</value>\n" +
                "            <units>MINUTES</units>\n" +
                "        </queryChanngeLogDiscoveryPeriod>\n" +
                "    </performanceTunning>\n" +
                "</queryManagerConfig>";


        final QueryManagerConfig config = QueryManagerConfigUnmarshaller.unmarshall(new StringReader(xml));
        assertNotNull(config);
    }

    @Test(expected = UnmarshalException.class)
    public void unmarshal_invalidXml() throws Exception {
        final String xml =
                "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" +
                "<queryManagerConfig>\n" +
                "    <queryChangeLogSource>\n" +
                "        <kafka>\n" +
                "            <hostname>localhost</hostname>\n" +
                "            <port>6</port>\n" +
                "        </kafka>\n" +
                "    </queryChangeLogSource>\n" +
                "</queryManagerConfig>";


        QueryManagerConfigUnmarshaller.unmarshall(new StringReader(xml));
    }
}