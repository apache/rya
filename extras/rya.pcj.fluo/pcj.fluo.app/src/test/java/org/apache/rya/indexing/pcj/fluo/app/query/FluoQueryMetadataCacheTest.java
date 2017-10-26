package org.apache.rya.indexing.pcj.fluo.app.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.fluo.api.client.Transaction;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.mockito.Mockito;

public class FluoQueryMetadataCacheTest {

    @Test
    public void testCache() {
        FluoQueryMetadataDAO mockDAO = Mockito.mock(FluoQueryMetadataDAO.class);
        Transaction mockTx = Mockito.mock(Transaction.class);
        String nodeId = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        StatementPatternMetadata metadata = StatementPatternMetadata.builder(nodeId).setParentNodeId("parent")
                .setStatementPattern("pattern").setVarOrder(new VariableOrder("xyz")).build();
        when(mockDAO.readStatementPatternMetadata(mockTx, nodeId)).thenReturn(metadata);

        FluoQueryMetadataCache cache = new FluoQueryMetadataCache(mockDAO, 20, 2);

        assertEquals(metadata, cache.readStatementPatternMetadata(mockTx, nodeId));

        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);

        Mockito.verify(mockDAO, Mockito.times(1)).readStatementPatternMetadata(mockTx, nodeId);
    }
}
