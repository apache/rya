package mvm.rya.blueprints.sail

import com.tinkerpop.blueprints.impls.sail.SailGraph
import com.tinkerpop.blueprints.impls.sail.SailHelper
import com.tinkerpop.blueprints.impls.sail.SailTokens
import org.openrdf.model.Literal
import org.openrdf.model.Resource
import org.openrdf.model.URI
import org.openrdf.model.Value
import org.openrdf.model.impl.BNodeImpl
import org.openrdf.model.impl.URIImpl
import org.openrdf.sail.Sail
import org.openrdf.sail.SailConnection
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.util.MultiIterable
import org.openrdf.sail.SailException

/**
 * Blueprints Graph to interact with Sail stores
 *
 * Date: 5/8/12
 * Time: 5:52 PM
 */
class RyaSailGraph extends SailGraph {

    public static final Resource[] EMPTY_CONTEXT = new Resource[0]

    RyaSailGraph(Sail sail) {
        super(sail)
    }

    /**
     * For some reason, the SailGraph does not implement this method.
     * The id is the full formatted id of the edge (rdf statement)
     *
     * @param id
     * @return
     */
    @Override
    Edge getEdge(Object id) {
        assert id != null
        return RyaSailEdge.fromId(id, this)
    }

    @Override
    Iterable<Edge> getEdges() {
        return getEdgesSequence();
    }

    protected RyaSailEdgeSequence getEdgesSequence() {
        return new RyaSailEdgeSequence(((SailConnection) sailConnection.get()).getStatements(null, null, null, false, new Resource[0]), this)
    }

    @Override
    Iterable<Vertex> getVertices() {
        return new RyaSailVertexSequence(this.getEdgesSequence())
    }

    /**
     * Utility method that can take a string and make it a Resource, Uri, or Literal
     * @param resource
     * @return
     */
    public Value createValue(String resource) {
        if (SailHelper.isBNode(resource))
            new BNodeImpl(resource.substring(2));
        Literal literal;
        if ((literal = SailHelper.makeLiteral(resource, this)) != null)
            return literal
        if (resource.contains(":") || resource.contains("/") || resource.contains("#")) {
            resource = expandPrefix(resource);
            new URIImpl(resource);
        } else {
            throw new RuntimeException((new StringBuilder()).append(resource).append(" is not a valid URI, blank node, or literal value").toString());
        }
    }

    public Vertex createVertex(String resource) {
        return new RyaSailVertex(createValue(resource), this);
    }

    @Override
    public Vertex addVertex(Object id) {
        if (null == id)
            id = SailTokens.URN_UUID_PREFIX + UUID.randomUUID().toString();
        return createVertex(id.toString());
    }

    @Override
    public Vertex getVertex(final Object id) {
        if (null == id)
            throw new IllegalArgumentException("Element identifier cannot be null");

        try {
            return createVertex(id.toString());
        } catch (RuntimeException re) {
            return null;
        }
    }

    public Iterable<Edge> query(final String subj, final String pred, final String obj, final String cntxt) {
        return new RyaSailEdgeSequence(sailConnection.get().getStatements(
                    (subj != null) ? (Resource) createValue(subj) : null, 
                    (pred != null) ? (URI) createValue(pred) : null,
                    (obj != null) ? createValue(obj) : null, 
                    false,
                    (cntxt != null) ? (Resource) createValue(cntxt) : EMPTY_CONTEXT),
                this);
    }

}
