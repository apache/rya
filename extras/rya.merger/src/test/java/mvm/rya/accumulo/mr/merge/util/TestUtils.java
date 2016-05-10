package mvm.rya.accumulo.mr.merge.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;

/**
 * Utility methods for testing merging/copying.
 */
public final class TestUtils {
    /**
     * 'LAST_MONTH' is the testing timestamp for all data that was in the both the parent and child when the child was copied from the parent.
     * So, after the child was created it should contain data with this timestamp too before merging.
     */
    public static final Date LAST_MONTH = new Date(System.currentTimeMillis() - (31L * 24L * 60L * 60L * 1000L));

    /**
     * 'YESTERDAY' is the testing start timestamp specified by the user to use for merging, all data with timestamps
     * after yesterday should be merged from the child to the parent.
     */
    public static final Date YESTERDAY = new Date(System.currentTimeMillis() - (24L * 60L * 60L * 1000L));

    /**
     * 'TODAY' is when the merge process is actually happening.
     */
    public static final Date TODAY = new Date();

    private static final String NAMESPACE = "#:";//"urn:x:x#"; //"urn:test:litdups#";

    /**
     * Private constructor to prevent instantiation.
     */
    private TestUtils() {
    }

    /**
     * Checks if a {@link RyaStatement} is in the specified instance's DAO.
     * @param description the description message for the statement.
     * @param verifyResultCount the expected number of matches.
     * @param matchStatement the {@link RyaStatement} to match.
     * @param dao the {@link AccumuloRyaDAO}.
     * @param config the {@link AccumuloRdfConfiguration} for the instance.
     * @throws RyaDAOException
     */
    public static void assertStatementInInstance(String description, int verifyResultCount, RyaStatement matchStatement, AccumuloRyaDAO dao, AccumuloRdfConfiguration config) throws RyaDAOException {
        CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(matchStatement, config);
        int count = 0;
        while (iter.hasNext()) {
            RyaStatement statement = iter.next();
            assertTrue(description + " - match subject: " + matchStatement,      matchStatement.getSubject().equals(statement.getSubject()));
            assertTrue(description + " - match predicate: " + matchStatement,    matchStatement.getPredicate().equals(statement.getPredicate()));
            assertTrue(description + " - match object match: " + matchStatement, matchStatement.getObject().equals(statement.getObject()));
            count++;
        }
        iter.close();
        assertEquals(description+" - Match Counts.", verifyResultCount, count);
    }

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    public static RyaURI createRyaUri(String localName) {
        return AccumuloRyaUtils.createRyaUri(NAMESPACE, localName);
    }

    /**
     * Creates a {@link RyaStatement} from the specified subject, predicate, and object.
     * @param subject the subject.
     * @param predicate the predicate.
     * @param object the object.
     * @param date the {@link Date} to use for the key's timestamp.
     * @return the {@link RyaStatement}.
     */
    public static RyaStatement createRyaStatement(String subject, String predicate, String object, Date date) {
        RyaURI subjectUri = createRyaUri(subject);
        RyaURI predicateUri = createRyaUri(predicate);
        RyaURI objectUri = createRyaUri(object);
        RyaStatement ryaStatement = new RyaStatement(subjectUri, predicateUri, objectUri);
        if (date != null) {
            ryaStatement.setTimestamp(date.getTime());
        }
        return ryaStatement;
    }

    /**
     * Copies a {@link RyaStatement} into a new {@link RyaStatement}.
     * @param s the {@link RyaStatement} to copy.
     * @return the newly copied {@link RyaStatement}.
     */
    public static RyaStatement copyRyaStatement(RyaStatement s) {
        return new RyaStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext(), s.getQualifer(), s.getColumnVisibility(), s.getValue(), s.getTimestamp());
    }
}