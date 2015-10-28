package mvm.rya.blueprints.sail

import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.tinkerpop.blueprints.Edge
import com.tinkerpop.blueprints.Vertex
import org.openrdf.model.Statement

/**
 * Iterable that provides a distinct list of subjects or objects from statements
 * Date: 5/8/12
 * Time: 5:56 PM
 */
class RyaSailVertexSequence implements Iterable<Vertex>, Iterator<Vertex> {
    enum VERTEXSIDE {
        SUBJECT, OBJECT
    }
    def PeekingIterator<Edge> iter
    def RyaSailGraph graph
    def previous
    def vertexSide = VERTEXSIDE.SUBJECT

    RyaSailVertexSequence() {
    }

    RyaSailVertexSequence(RyaSailEdgeSequence iter) {
        this(iter, VERTEXSIDE.SUBJECT)
    }
    
    RyaSailVertexSequence(RyaSailEdgeSequence iter, VERTEXSIDE vertexSide) {
        this.iter = Iterators.peekingIterator(iter)
        this.graph = iter.graph
        this.vertexSide = vertexSide
    }

    @Override
    Iterator<Vertex> iterator() {
        return this
    }

    @Override
    boolean hasNext() {
        if (iter == null) {
            return false
        }
        while (iter.hasNext()) {
            def peek = (RyaSailEdge) iter.peek()
            def subject = getVertexSide(peek.getRawEdge())
            if (!(subject.equals(previous))) {
                return true
            }
            iter.next() //keep iterating
        }
        return false;
    }

    @Override
    Vertex next() {
        if (!this.hasNext())
            throw new NoSuchElementException();
        def next = (RyaSailEdge) iter.next()
        Statement statement = next.getRawEdge()
        previous = getVertexSide(statement)
        return new RyaSailVertex(previous, graph);
    }

    def getVertexSide(Statement statement) {
        return (VERTEXSIDE.SUBJECT.equals(vertexSide)) ? statement.getSubject() : statement.getObject()
    }

    @Override
    void remove() {
        throw new UnsupportedOperationException();
    }
}
