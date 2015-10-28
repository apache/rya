package mvm.rya.accumulo.experimental;

import java.io.IOException;

import mvm.rya.api.persist.index.RyaSecondaryIndexer;

import org.apache.accumulo.core.client.MultiTableBatchWriter;

public interface AccumuloIndexer extends RyaSecondaryIndexer {
    
    public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException;

}