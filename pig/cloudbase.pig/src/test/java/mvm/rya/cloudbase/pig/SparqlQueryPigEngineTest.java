package mvm.rya.cloudbase.pig;

import junit.framework.TestCase;
import mvm.rya.cloudbase.pig.SparqlQueryPigEngine;
import mvm.rya.cloudbase.pig.SparqlToPigTransformVisitor;
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
        assertNotNull(engine.generatePigScript(query));
    }
}
