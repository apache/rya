package mvm.rya.cloudbase;

import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import com.google.common.base.Preconditions;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.persist.RdfDAOException;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;

import java.io.IOError;
import java.util.Iterator;
import java.util.Map.Entry;

public class CloudbaseNamespaceTableIterator<T extends Namespace> implements
        CloseableIteration<Namespace, RdfDAOException> {

    private boolean open = false;
    private Iterator<Entry<Key, Value>> result;

    public CloudbaseNamespaceTableIterator(Iterator<Entry<Key, Value>> result) throws RdfDAOException {
        Preconditions.checkNotNull(result);
        open = true;
        this.result = result;
    }

    @Override
    public void close() throws RdfDAOException {
        try {
            verifyIsOpen();
            open = false;
        } catch (IOError e) {
            throw new RdfDAOException(e);
        }
    }

    public void verifyIsOpen() throws RdfDAOException {
        if (!open) {
            throw new RdfDAOException("Iterator not open");
        }
    }

    @Override
    public boolean hasNext() throws RdfDAOException {
        verifyIsOpen();
        return result != null && result.hasNext();
    }

    @Override
    public Namespace next() throws RdfDAOException {
        if (hasNext()) {
            return getNamespace(result);
        }
        return null;
    }

    public static Namespace getNamespace(Iterator<Entry<Key, Value>> rowResults) {
        for (; rowResults.hasNext(); ) {
            Entry<Key, Value> next = rowResults.next();
            Key key = next.getKey();
            Value val = next.getValue();
            String cf = key.getColumnFamily().toString();
            String cq = key.getColumnQualifier().toString();
            return new NamespaceImpl(key.getRow().toString(), new String(
                    val.get()));
        }
        return null;
    }

    @Override
    public void remove() throws RdfDAOException {
        next();
    }

    public boolean isOpen() {
        return open;
    }
}
