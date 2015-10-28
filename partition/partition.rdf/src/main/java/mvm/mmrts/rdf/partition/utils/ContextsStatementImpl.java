package mvm.mmrts.rdf.partition.utils;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;

/**
 * Class ContextsStatementImpl
 * Date: Aug 5, 2011
 * Time: 7:48:56 AM
 */
public class ContextsStatementImpl extends StatementImpl {
    private Resource[] contexts;

    public ContextsStatementImpl(Resource subject, URI predicate, Value object, Resource... contexts) {
        super(subject, predicate, object);
        this.contexts = contexts;
    }

    public Resource[] getContexts() {
        return contexts;
    }

    @Override
    public Resource getContext() {
        //return first context in array
        return (contexts != null && contexts.length > 0) ? contexts[0] : null;
    }
}
