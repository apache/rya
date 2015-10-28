package mvm.mmrts.rdf.partition.iterators;

import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.sail.SailException;

import java.io.IOError;
import java.util.Iterator;
import java.util.Map.Entry;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

//TODO: Combine with CloudbaseStoreContextTableIterator4
public class NamespaceIterator implements
        CloseableIteration<Namespace, SailException> {

    private boolean open = false;
    private Iterator<Entry<Key, Value>> result;

    public NamespaceIterator(Connector connector, String table) throws SailException {
        initialize(connector, table);
        open = true;
    }

    protected void initialize(Connector connector, String table) throws SailException {
        try {
            Scanner scanner = connector.createScanner(table,
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumnFamily(NAMESPACE);
            result = scanner.iterator();
        } catch (TableNotFoundException e) {
            throw new SailException("Exception occurred in Namespace Iterator",
                    e);
        }
    }

    @Override
    public void close() throws SailException {
        try {
            verifyIsOpen();
            open = false;
        } catch (IOError e) {
            throw new SailException(e);
        }
    }

    public void verifyIsOpen() throws SailException {
        if (!open) {
            throw new SailException("Iterator not open");
        }
    }

    @Override
    public boolean hasNext() throws SailException {
        verifyIsOpen();
        return result != null && result.hasNext();
    }

    @Override
    public Namespace next() throws SailException {
        if (hasNext()) {
            Namespace namespace = getNamespace(result);
            return namespace;
        }
        return null;
    }

    public static Namespace getNamespace(Iterator<Entry<Key, Value>> rowResults) {
        for (; rowResults.hasNext();) {
            Entry<Key, Value> next = rowResults.next();
            Key key = next.getKey();
            String cq = key.getColumnQualifier().toString();
            return new NamespaceImpl(key.getRow().toString(), cq.toString());
        }

        return null;
    }

    @Override
    public void remove() throws SailException {
        next();
    }

    public boolean isOpen() {
        return open;
    }
}
