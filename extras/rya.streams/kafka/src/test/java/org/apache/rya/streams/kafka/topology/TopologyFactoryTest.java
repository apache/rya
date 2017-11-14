package org.apache.rya.streams.kafka.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.rya.streams.kafka.topology.TopologyFactory.ProcessorEntry;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

public class TopologyFactoryTest {
    private static TopologyFactory FACTORY;

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final Var TALKS_TO = new Var("-const-urn:talksTo", VF.createURI("urn:talksTo"));
    private static final Var CHEWS = new Var("-const-urn:chews", VF.createURI("urn:chews"));

    static {
        TALKS_TO.setAnonymous(true);
        TALKS_TO.setConstant(true);
        CHEWS.setAnonymous(true);
        CHEWS.setConstant(true);
    }

    @Before
    public void setup() {
        FACTORY = new TopologyFactory();
    }

    @Test
    public void projectionStatementPattern() throws Exception {
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
                + "}";

        FACTORY.build(query, "source", "sink");
        final List<ProcessorEntry> entries = FACTORY.getProcessorEntry();

        assertTrue(entries.get(0).getNode() instanceof Projection);
        assertTrue(entries.get(1).getNode() instanceof StatementPattern);

        final StatementPattern expected = new StatementPattern(new Var("person"), TALKS_TO, new Var("otherPerson"));
        assertEquals(expected, entries.get(1).getNode());
    }

    @Test
    public void projectionJoinStatementPattern() throws Exception {
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
                + "?otherPerson <urn:talksTo> ?dog . "
                + "}";

        FACTORY.build(query, "source", "sink");
        final List<ProcessorEntry> entries = FACTORY.getProcessorEntry();

        assertTrue(entries.get(0).getNode() instanceof Projection);
        assertTrue(entries.get(1).getNode() instanceof Join);
        StatementPattern expected = new StatementPattern(new Var("person"), TALKS_TO, new Var("otherPerson"));
        assertEquals(expected, entries.get(2).getNode());
        expected = new StatementPattern(new Var("otherPerson"), TALKS_TO, new Var("dog"));
        assertEquals(expected, entries.get(3).getNode());
    }

    @Test
    public void projectionJoinJoinStatementPattern() throws Exception {
        final String query = "SELECT * WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
                + "?otherPerson <urn:talksTo> ?dog . "
                + "?dog <urn:chews> ?toy . "
                + "}";

        FACTORY.build(query, "source", "sink");
        final List<ProcessorEntry> entries = FACTORY.getProcessorEntry();

        assertTrue(entries.get(0).getNode() instanceof Projection);
        assertTrue(entries.get(1).getNode() instanceof Join);
        assertTrue(entries.get(2).getNode() instanceof Join);
        StatementPattern expected = new StatementPattern(new Var("person"), TALKS_TO, new Var("otherPerson"));
        assertEquals(expected, entries.get(3).getNode());
        expected = new StatementPattern(new Var("otherPerson"), TALKS_TO, new Var("dog"));
        assertEquals(expected, entries.get(4).getNode());
        expected = new StatementPattern(new Var("dog"), CHEWS, new Var("toy"));
        assertEquals(expected, entries.get(5).getNode());
    }

    @Test
    public void projectionStatementPattern_rebind() throws Exception {
        final String query = "CONSTRUCT { ?person <urn:mightKnow> ?otherPerson } WHERE { "
                + "?person <urn:talksTo> ?otherPerson . "
            + "}";

        FACTORY.build(query, "source", "sink");
        final List<ProcessorEntry> entries = FACTORY.getProcessorEntry();

        assertTrue(entries.get(0).getNode() instanceof Projection);
        final StatementPattern expected = new StatementPattern(new Var("person"), TALKS_TO, new Var("otherPerson"));
        assertEquals(expected, entries.get(1).getNode());
    }
}
