package mvm.mmrts.rdf.partition.utils;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import junit.framework.TestCase;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import static mvm.mmrts.rdf.partition.utils.RdfIO.*;

/**
 * Class RdfIOTest
 * Date: Jul 6, 2011
 * Time: 12:59:25 PM
 */
public class RdfIOTest extends TestCase {

    ValueFactory vf = ValueFactoryImpl.getInstance();

    public void testWriteStatementEvent() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        byte[] bytes = writeStatement(stmt, true);
    }

    public void testWriteStatementIndex() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        byte[] bytes = writeStatement(stmt, false);
    }

    public void testExtraInfoInStmtBytes() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        byte[] bytes = writeStatement(stmt, true);
        bytes = Bytes.concat(bytes, "extrainformation".getBytes());
        Statement readStmt = readStatement(ByteStreams.newDataInput(bytes), ValueFactoryImpl.getInstance());
        System.out.println(readStmt);
    }

    public void testReadStatement() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        byte[] bytes = writeStatement(stmt, true);

        Statement readStmt = readStatement(ByteStreams.newDataInput(bytes), vf);
        assertEquals(readStmt, stmt);

        //testing blank node
        stmt = new StatementImpl(vf.createBNode("a12345"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        bytes = writeStatement(stmt, true);

        readStmt = readStatement(ByteStreams.newDataInput(bytes), vf);
        assertEquals(readStmt, stmt);

        //testing boolean literal datatype
        stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral(true));
        bytes = writeStatement(stmt, true);

        readStmt = readStatement(ByteStreams.newDataInput(bytes), vf);
        assertEquals(readStmt, stmt);
        
        //testing boolean literal datatype
        stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("label", "language"));
        bytes = writeStatement(stmt, true);

        readStmt = readStatement(ByteStreams.newDataInput(bytes), vf);
        assertEquals(readStmt, stmt);
    }

    public void testReadIndexStatement() throws Exception {
        StatementImpl stmt = new StatementImpl(vf.createURI("urn:namespace#subj"), vf.createURI("urn:namespace#pred"), vf.createLiteral("object"));
        byte[] bytes = writeStatement(stmt, false);

        Statement readStmt = readStatement(ByteStreams.newDataInput(bytes), vf, false);
        assertEquals(readStmt, stmt);

        bytes = writeStatement(stmt, true);

        readStmt = readStatement(ByteStreams.newDataInput(bytes), vf, true);
        assertEquals(readStmt, stmt);
    }

}
