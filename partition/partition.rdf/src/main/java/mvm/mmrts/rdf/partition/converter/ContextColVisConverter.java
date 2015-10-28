package mvm.mmrts.rdf.partition.converter;

import cloudbase.core.security.ColumnVisibility;
import org.openrdf.model.Resource;

/**
 * Interface ContextColVisConverter
 * Date: Aug 5, 2011
 * Time: 7:35:40 AM
 */
public interface ContextColVisConverter {

    public ColumnVisibility convertContexts(Resource... contexts);
}
