package org.apache.rya.accumulo.pig;

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



import junit.framework.TestCase;
import org.apache.pig.ExecType;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/23/12
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class SparqlQueryPigEngineTest extends TestCase {

    private SparqlQueryPigEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SparqlToPigTransformVisitor visitor = new SparqlToPigTransformVisitor();
        visitor.setTablePrefix("l_");
        visitor.setInstance("stratus");
        visitor.setZk("stratus13:2181");
        visitor.setUser("root");
        visitor.setPassword("password");

        engine = new SparqlQueryPigEngine();
        engine.setSparqlToPigTransformVisitor(visitor);
        engine.setExecType(ExecType.LOCAL);
        engine.setInference(false);
        engine.setStats(false);
        engine.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        engine.destroy();
    }

    public void testStatementPattern() throws Exception {
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                " PREFIX ub: <urn:lubm:rdfts#>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                " SELECT * WHERE\n" +
                " {\n" +
                "\t<http://www.Department0.University0.edu> ?p ?o\n" +
                " }\n" +
                "";

//        engine.runQuery(query, "/temp/testSP");
        assertTrue(engine.generatePigScript(query).contains("A = load 'accumulo://l_?instance=stratus&user=root&password=password&zookeepers=stratus13:2181&subject=<http://www.Department0.University0.edu>' using org.apache.rya.accumulo.pig.StatementPatternStorage() AS (A_s:chararray, p:chararray, o:chararray);\n" +
                "PROJ = FOREACH A GENERATE p,o;"));

    }
}
