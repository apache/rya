package mvm.rya.accumulo.experimental;

import java.io.IOException;
import java.util.Collection;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;

import org.apache.accumulo.core.client.MultiTableBatchWriter;

public abstract class AbstractAccumuloIndexer implements AccumuloIndexer {

    @Override
    public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException {
    }

    @Override
    public void storeStatements(Collection<RyaStatement> statements) throws IOException {
        for (RyaStatement s : statements) {
            storeStatement(s);
        }
    }

    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
    }

    @Override
    public void dropGraph(RyaURI... graphs) {
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
}
