package org.apache.rya.indexing.export.mongo2mongo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.ExportStatementMerger;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.time.TimeMongoRyaStatementStore;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.apache.rya.indexing.export.ITBase;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.repository.RepositoryException;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.MongoDBRyaDAO;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;

public class MongoToMongoIT extends ITBase {
    private static final String RYA_INSTANCE = "ryaInstance";
    private MongoClient parentClient;
    private MongoClient childClient;

    private MongoRyaStatementStore parentStore;
    private MongoRyaStatementStore childStore;

    private MongoParentMetadataRepository parentMetadata;
    private MongoParentMetadataRepository childMetadata;

    private MongoDBRyaDAO parentDAO;
    private MongoDBRyaDAO childDAO;

    @Before
    public void setupMongos() throws MongoException, NumberFormatException, RepositoryException, AccumuloException, AccumuloSecurityException, RyaDAOException, InferenceEngineException, IOException {
        parentClient = getnewMongoResources(RYA_INSTANCE);
        parentDAO = new MongoDBRyaDAO(getConf(parentClient), parentClient);
        parentStore = new MongoRyaStatementStore(parentClient, RYA_INSTANCE, parentDAO);
        parentMetadata = new MongoParentMetadataRepository(parentClient, RYA_INSTANCE);

        childClient = getnewMongoResources(RYA_INSTANCE);
        childDAO = new MongoDBRyaDAO(getConf(childClient), childClient);
        childStore = new MongoRyaStatementStore(childClient, RYA_INSTANCE, childDAO);
        childMetadata = new MongoParentMetadataRepository(childClient, RYA_INSTANCE);
    }

    @Test
    public void emptyMergeTest() throws AddStatementException {
        final Date currentTime = new Date(0L);
        final TimeMongoRyaStatementStore timeStore = new TimeMongoRyaStatementStore(parentStore, currentTime, RYA_INSTANCE);
        loadMockStatements(parentStore, 50);

        final MemoryTimeMerger merger = new MemoryTimeMerger(timeStore, childStore,
                parentMetadata, childMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        merger.runJob();
        assertEquals(50, count(childStore));
    }

    @Test
    public void no_statementsTest() throws AddStatementException {
        loadMockStatements(parentStore, 50);
        Date currentTime = new Date();
        //ensure current time is later
        currentTime = new Date(currentTime.getTime() + 10000L);

        final TimeMongoRyaStatementStore timeStore = new TimeMongoRyaStatementStore(parentStore, currentTime, RYA_INSTANCE);
        final MemoryTimeMerger merger = new MemoryTimeMerger(timeStore, childStore,
                parentMetadata, childMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        merger.runJob();
        assertEquals(0, count(childStore));
    }

    @Test
    public void childToParent_ChildAddTest() throws AddStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        merger.runJob();

        //add a few statements to child
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://51");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://52");
        childStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        otherMerger.runJob();
        assertEquals(52, count(parentStore));
    }

    @Test
    public void childToParent_ChildReAddsDeletedStatementTest() throws AddStatementException, RemoveStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        merger.runJob();

        //remove a statement from the parent
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://1");
        parentStore.removeStatement(stmnt1);
        assertEquals(49, count(parentStore));

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        otherMerger.runJob();
        //merging will have added the statement back
        assertEquals(50, count(parentStore));
    }

    @Test
    public void childToParent_BothAddTest() throws AddStatementException {
        //get the timestamp now.
        final Date currentTime = new Date();
        loadMockStatements(parentStore, 50);

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                parentMetadata, childMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        merger.runJob();

        //add a statement to each store
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://51");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://52");
        parentStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);


        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                childMetadata, parentMetadata, new ExportStatementMerger(), currentTime, RYA_INSTANCE);
        otherMerger.runJob();
        //both should still be there
        assertEquals(52, count(parentStore));
    }

    private void loadMockStatements(final RyaStatementStore store, final int count) throws AddStatementException {
        for(int ii = 0; ii < count; ii++) {
            final RyaStatement statement = makeRyaStatement("http://subject", "http://predicate", "http://"+ii);
            store.addStatement(statement);
        }
    }

    private int count(final RyaStatementStore store) {
        final Iterator<RyaStatement> statements = store.fetchStatements();
        int count = 0;
        while(statements.hasNext()) {
            statements.next();
            count++;
        }
        return count;
    }
}
