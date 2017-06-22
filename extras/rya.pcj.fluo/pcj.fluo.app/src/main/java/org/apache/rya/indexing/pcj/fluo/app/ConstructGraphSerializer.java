package org.apache.rya.indexing.pcj.fluo.app;
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
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Joiner;

/**
 * Converts {@link ConstructGraph}s to and from Strings for
 * storage and retrieval from Fluo. 
 *
 */
public class ConstructGraphSerializer {

    public static final String SP_DELIM = "\u0002";
    
    public static ConstructGraph toConstructGraph(String graphString) {
        Set<ConstructProjection> projections = new HashSet<>();
        String[] spStrings = graphString.split(SP_DELIM);
        for(String sp: spStrings) {
           projections.add(new ConstructProjection(FluoStringConverter.toStatementPattern(sp))); 
        }
        return new ConstructGraph(projections);
    }
    
    public static String toConstructString(ConstructGraph graph) {
        Set<ConstructProjection> projections = graph.getProjections();
        Set<String> spStrings = new HashSet<>();
        for(ConstructProjection projection: projections) {
            spStrings.add(FluoStringConverter.toStatementPatternString(projection.getStatementPatternRepresentation()));
        }
        return Joiner.on(SP_DELIM).join(spStrings);
    }
    
}
