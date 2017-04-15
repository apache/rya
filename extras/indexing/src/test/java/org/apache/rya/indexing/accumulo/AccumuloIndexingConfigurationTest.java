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
package org.apache.rya.indexing.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.api.domain.RyaURI;
import org.junit.Test;

public class AccumuloIndexingConfigurationTest {

    @Test
    public void testBuilder() {
        String prefix = "rya_";
        String auth = "U";
        String visibility = "U";
        String user = "user";
        String password = "password";
        String instance = "instance";
        String zookeeper = "zookeeper";
        boolean useMock = false;
        boolean useComposite = true;
        boolean usePrefixHash = true;
        boolean useInference = true;
        boolean displayPlan = false;

        AccumuloIndexingConfiguration conf = AccumuloIndexingConfiguration.builder()
                .setAuths(auth)
                .setVisibilities(visibility)
                .setRyaPrefix(prefix)
                .setUseInference(useInference)
                .setUseCompositeCardinality(useComposite)
                .setDisplayQueryPlan(displayPlan)
                .setAccumuloInstance(instance)
                .setAccumuloPassword(password)
                .setAccumuloUser(user)
                .setAccumuloZooKeepers(zookeeper)
                .setUseMockAccumulo(useMock)
                .setUseAccumuloPrefixHashing(usePrefixHash)
                .setAccumuloFreeTextPredicates("http://pred1", "http://pred2")
                .setAccumuloTemporalPredicates("http://pred3", "http://pred4")
                .setUseAccumuloTemporalIndex(true)
                .setUseAccumuloEntityIndex(true)
                .setUseAccumuloFreetextIndex(true)
                .setPcjUpdaterFluoAppName("fluo")
                .setUseOptimalPcj(true)
                .setPcjTables("table1", "table2").build();

        assertEquals(conf.getTablePrefix(), prefix);
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isUseCompositeCardinality(), useComposite);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getAccumuloInstance(), instance);
        assertEquals(conf.getAccumuloPassword(), password);
        assertEquals(conf.getAccumuloUser(), user);
        assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        assertEquals(conf.getUseMockAccumulo(), useMock);
        assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);
        assertTrue(
                Arrays.equals(conf.getAccumuloFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        assertTrue(
                Arrays.equals(conf.getAccumuloTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));
        assertEquals(conf.getPcjTables(), Arrays.asList("table1", "table2"));
        assertEquals(conf.getUsePCJ(), false);
        assertEquals(conf.getUseOptimalPCJ(), true);
        assertEquals(conf.getUseEntity(), true);
        assertEquals(conf.getUseFreetext(), true);
        assertEquals(conf.getUseTemporal(), true);
        assertEquals(conf.getUsePCJUpdater(), true);
        assertEquals(conf.getFluoAppUpdaterName(), "fluo");

    }

    @Test
    public void testBuilderFromProperties() throws FileNotFoundException, IOException {
        String prefix = "rya_";
        String auth = "U";
        String visibility = "U";
        String user = "user";
        String password = "password";
        String instance = "instance";
        String zookeeper = "zookeeper";
        boolean useMock = false;
        boolean useComposite = true;
        boolean usePrefixHash = true;
        boolean useInference = true;
        boolean displayPlan = false;
        boolean useMetadata = true;
        Set<RyaURI> metaProperties = new HashSet<>(Arrays.asList(new RyaURI("urn:123"), new RyaURI("urn:456"))); 
        

        Properties props = new Properties();
        props.load(new FileInputStream("src/test/resources/accumulo_rya_indexing.properties"));

        AccumuloIndexingConfiguration conf = AccumuloIndexingConfiguration.fromProperties(props);

        assertEquals(conf.getTablePrefix(), prefix);
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isUseCompositeCardinality(), useComposite);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getAccumuloInstance(), instance);
        assertEquals(conf.getAccumuloPassword(), password);
        assertEquals(conf.getAccumuloUser(), user);
        assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        assertEquals(conf.getUseMockAccumulo(), useMock);
        assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);
        assertTrue(
                Arrays.equals(conf.getAccumuloFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        assertTrue(
                Arrays.equals(conf.getAccumuloTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));
        assertEquals(conf.getPcjTables(), Arrays.asList("table1", "table2"));
        assertEquals(conf.getUsePCJ(), false);
        assertEquals(conf.getUseOptimalPCJ(), true);
        assertEquals(conf.getUseEntity(), true);
        assertEquals(conf.getUseFreetext(), true);
        assertEquals(conf.getUseTemporal(), true);
        assertEquals(conf.getUsePCJUpdater(), true);
        assertEquals(conf.getFluoAppUpdaterName(), "fluo");
        assertEquals(conf.getUseStatementMetadata(), useMetadata);
        assertEquals(conf.getStatementMetadataProperties(), metaProperties);

    }

}
