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

package mvm.rya.blueprints.sail

import mvm.rya.api.utils.IteratorWrapper
import junit.framework.TestCase
import mvm.rya.blueprints.config.RyaGraphConfiguration
import org.openrdf.model.Statement
import static mvm.rya.accumulo.mr.MRUtils.*
import static mvm.rya.api.RdfCloudTripleStoreConstants.VALUE_FACTORY

/**
 * Date: 5/10/12
 * Time: 8:55 AM
 */
class RyaSailVertexSequenceTest extends TestCase {

    public void testDistinctSubjects() {
        def namespace = "urn:test#"
        def vf = VALUE_FACTORY
        def graph = RyaGraphConfiguration.createGraph(
                [(AC_INSTANCE_PROP): "inst",
                        (AC_MOCK_PROP): "true",
                        (AC_USERNAME_PROP): "user",
                        (AC_PWD_PROP): "pwd",
                ]
        );

        def a = vf.createURI(namespace, "a")
        def b = vf.createURI(namespace, "b")
        def c = vf.createURI(namespace, "c")
        def statements = [
                vf.createStatement(a, vf.createURI(namespace, "p"), vf.createURI(namespace, "l1")),
                vf.createStatement(a, vf.createURI(namespace, "p"), vf.createURI(namespace, "l2")),
                vf.createStatement(a, vf.createURI(namespace, "p"), vf.createURI(namespace, "l3")),
                vf.createStatement(b, vf.createURI(namespace, "p"), vf.createURI(namespace, "l1")),
                vf.createStatement(c, vf.createURI(namespace, "p"), vf.createURI(namespace, "l1")),
                vf.createStatement(c, vf.createURI(namespace, "p"), vf.createURI(namespace, "l2")),
                vf.createStatement(c, vf.createURI(namespace, "p"), vf.createURI(namespace, "l3")),
        ]
        def edgeSeq = new RyaSailEdgeSequence(new IteratorWrapper<Statement>(statements.iterator()), graph)
        def vertexSeq = new RyaSailVertexSequence(edgeSeq)
        def expectedList = [a, b, c]
        def list = vertexSeq.toList().collect { v ->
            v.getRawVertex()
        }
        assertEquals(expectedList, list)
    }

    public void testDistinctObjects() {
        def namespace = "urn:test#"
        def vf = VALUE_FACTORY
        def graph = RyaGraphConfiguration.createGraph(
                [(AC_INSTANCE_PROP): "inst",
                        (AC_MOCK_PROP): "true",
                        (AC_USERNAME_PROP): "user",
                        (AC_PWD_PROP): "pwd",
                ]
        );
        def a = vf.createURI(namespace, "a")
        def b = vf.createURI(namespace, "b")
        def c = vf.createURI(namespace, "c")
        def l1 = vf.createURI(namespace, "l1")
        def l2 = vf.createURI(namespace, "l2")
        def l3 = vf.createURI(namespace, "l3")
        def statements = [
                vf.createStatement(a, vf.createURI(namespace, "p"), l1),
                vf.createStatement(b, vf.createURI(namespace, "p"), l1),
                vf.createStatement(c, vf.createURI(namespace, "p"), l1),
                vf.createStatement(a, vf.createURI(namespace, "p"), l2),
                vf.createStatement(c, vf.createURI(namespace, "p"), l2),
                vf.createStatement(a, vf.createURI(namespace, "p"), l3),
                vf.createStatement(c, vf.createURI(namespace, "p"), l3),
        ]
        def edgeSeq = new RyaSailEdgeSequence(new IteratorWrapper<Statement>(statements.iterator()), graph)
        def vertexSeq = new RyaSailVertexSequence(edgeSeq, RyaSailVertexSequence.VERTEXSIDE.OBJECT)
        def expectedList = [l1, l2, l3]
        def list = vertexSeq.toList().collect { v ->
            v.getRawVertex()
        }
        assertEquals(expectedList, list)
    }
}
