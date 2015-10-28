package mvm.rya.indexing.mongodb;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.Statement;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.index.RyaSecondaryIndexer;

public abstract class AbstractMongoIndexer implements RyaSecondaryIndexer {

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush() throws IOException {
    }


    @Override
    public Configuration getConf() {
        return null;
    }


    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void storeStatements(Collection<RyaStatement> ryaStatements)
            throws IOException {
        for (RyaStatement ryaStatement : ryaStatements){
            storeStatement(ryaStatement);
        }
        
    }

    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropGraph(RyaURI... graphs) {
        throw new UnsupportedOperationException();
    }

}
