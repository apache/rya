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
package org.apache.rya.indexing.pcj.functions.geo;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ClientCnxn;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;

/**
 * Verifies that the geoFunctions are registered via SPI.
 * Also see the more detailed integration test.
 */
public class GeoFunctionsTest {
    @Before
    public void before() {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger(ClientCnxn.class).setLevel(Level.OFF);
    }

    /**
     * Thirty-some functions are registered via SPI. Make sure they are registered.
     * This file lists functions to load:
     * src/main/resources/META-INF/services/ org.eclipse.rdf4j.query.algebra.evaluation.function.Function
     */
    @Test
    public void verifySpiLoadedGeoFunctions() {
        final String functions[] = { "distance", //
                "convexHull", "boundary", "envelope", "union", "intersection", "symDifference", "difference", //
                "relate", /* "equals", */ "sfDisjoint", "sfIntersects", "sfTouches", "sfCrosses", //
                "sfWithin", "sfContains", "sfOverlaps", "ehDisjoint", "ehMeet", "ehOverlap", //
                "ehCovers", "ehCoveredBy", "ehInside", "ehContains", "rcc8dc", "rcc8ec", //
                "rcc8po", "rcc8tppi", "rcc8tpp", "rcc8ntpp", "rcc8ntppi" }; //
        HashSet<String> functionsCheckList = new HashSet<String>();
        functionsCheckList.addAll(Arrays.asList(functions));
        for (String f : FunctionRegistry.getInstance().getKeys()) {
            String functionShortName = f.replaceFirst("^.*/geosparql/(.*)", "$1");
            // System.out.println("Registered function: " + f + " shortname: " + functionShortName);
            functionsCheckList.remove(functionShortName);
        }
        assertTrue("Missed loading these functions via SPI: " + functionsCheckList, functionsCheckList.isEmpty());
    }

}
