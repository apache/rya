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

//package mvm.rya.blueprints.sail
//
//import com.tinkerpop.blueprints.pgm.impls.MultiIterable
//import com.tinkerpop.blueprints.pgm.impls.sail.SailVertex
//import org.openrdf.model.Resource
//import org.openrdf.model.Value
//import org.openrdf.model.impl.URIImpl
//import org.openrdf.sail.SailException
//import com.tinkerpop.blueprints.pgm.Edge
//
///**
// * Extension to SailVertex to use RyaSailEdgeSequence underneath
// * Date: 5/9/12
// * Time: 3:40 PM
// */
//class RyaSailVertex extends SailVertex {
//
//    def sailGraph
//
//    RyaSailVertex(Value rawVertex, RyaSailGraph graph) {
//        super(rawVertex, graph)
//        sailGraph = graph
//    }
//
//    @Override
//    public Iterable<Edge> getOutEdges(final String... labels) {
//        def vertex = getRawVertex()
//        if (vertex instanceof Resource) {
//            try {
//                if (labels.length == 0) {
//                    return new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements((Resource) vertex, null, null, false), sailGraph);
//                } else if (labels.length == 1) {
//                    return new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements((Resource) vertex, new URIImpl(sailGraph.expandPrefix(labels[0])), null, false), sailGraph);
//                } else {
//                    final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
//                    for (final String label: labels) {
//                        edges.add(new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements((Resource) vertex, new URIImpl(sailGraph.expandPrefix(label)), null, false), sailGraph));
//                    }
//                    return new MultiIterable<Edge>(edges);
//                }
//            } catch (SailException e) {
//                throw new RuntimeException(e.getMessage(), e);
//            }
//        } else {
//            return new RyaSailEdgeSequence();
//        }
//
//    }
//
//    @Override
//    public Iterable<Edge> getInEdges(final String... labels) {
//        try {
//            def vertex = getRawVertex()
//            if (labels.length == 0) {
//                return new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements(null, null, vertex, false), sailGraph);
//            } else if (labels.length == 1) {
//                return new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements(null, new URIImpl(sailGraph.expandPrefix(labels[0])), vertex, false), sailGraph);
//            } else {
//                final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
//                for (final String label: labels) {
//                    edges.add(new RyaSailEdgeSequence(sailGraph.getSailConnection().get().getStatements(null, new URIImpl(sailGraph.expandPrefix(label)), vertex, false), sailGraph));
//                }
//                return new MultiIterable<Edge>(edges);
//            }
//        } catch (SailException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//
//    }
//}
