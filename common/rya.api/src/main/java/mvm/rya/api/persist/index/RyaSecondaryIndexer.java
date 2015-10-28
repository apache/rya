package mvm.rya.api.persist.index;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;

import org.apache.hadoop.conf.Configurable;

public interface RyaSecondaryIndexer extends Closeable, Flushable, Configurable {

    public String getTableName();

    public void storeStatements(Collection<RyaStatement> statements) throws IOException;

    public void storeStatement(RyaStatement statement) throws IOException;

    public void deleteStatement(RyaStatement stmt) throws IOException;

    public void dropGraph(RyaURI... graphs);

}
