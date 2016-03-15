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

package mvm.rya.blueprints.sail;

/*
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
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.util.MultiIterable;
import com.tinkerpop.blueprints.impls.sail.SailGraph;
import com.tinkerpop.blueprints.impls.sail.SailVertex;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * For some reason, the groovy class overwrites the metaclass and Gremlin.load will not change it for properties like outE, both, etc
 * Date: 5/10/12
 * Time: 12:35 PM
 */
public class RyaSailVertex extends SailVertex {

    public RyaSailVertex(Value rawVertex, SailGraph graph) {
        super(rawVertex, graph);
    }

    
    @Override
    public Iterable<Edge> getEdges(Direction direction, final String... labels) {
        if (direction.equals(Direction.OUT))
            return getOutEdges(labels);
        if (direction.equals(Direction.IN)) {
            return getInEdges(labels);
        }
        return new MultiIterable(Arrays.asList(new Iterable[] { getInEdges(labels), getOutEdges(labels) }));
    }

    private Iterable<Edge> getOutEdges(final String... labels) {
        Value vertex = getRawVertex();
        if (vertex instanceof Resource) {
            try {
                if (labels.length == 0) {
                    return new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements((Resource) vertex, null, null, false), getRyaSailGraph());
                } else if (labels.length == 1) {
                    return new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements((Resource) vertex, new URIImpl(getRyaSailGraph().expandPrefix(labels[0])), null, false), getRyaSailGraph());
                } else {
                    final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
                    for (final String label: labels) {
                        edges.add(new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements((Resource) vertex, new URIImpl(getRyaSailGraph().expandPrefix(label)), null, false), getRyaSailGraph()));
                    }
                    return new MultiIterable<Edge>(edges);
                }
            } catch (SailException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            return new RyaSailEdgeSequence();
        }
    }

    private Iterable<Edge> getInEdges(final String... labels) {
        try {
            Value vertex = getRawVertex();
            if (labels.length == 0) {
                return new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements(null, null, vertex, false), getRyaSailGraph());
            } else if (labels.length == 1) {
                return new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements(null, new URIImpl(getRyaSailGraph().expandPrefix(labels[0])), vertex, false), getRyaSailGraph());
            } else {
                final List<Iterable<Edge>> edges = new ArrayList<Iterable<Edge>>();
                for (final String label: labels) {
                    edges.add(new RyaSailEdgeSequence(getRyaSailGraph().getSailConnection().get().getStatements(null, new URIImpl(getRyaSailGraph().expandPrefix(label)), vertex, false),getRyaSailGraph()));
                }
                return new MultiIterable<Edge>(edges);
            }
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public RyaSailGraph getRyaSailGraph() {
        return (RyaSailGraph) graph;
    }
}
