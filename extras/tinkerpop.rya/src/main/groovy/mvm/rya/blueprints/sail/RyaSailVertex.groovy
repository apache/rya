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
