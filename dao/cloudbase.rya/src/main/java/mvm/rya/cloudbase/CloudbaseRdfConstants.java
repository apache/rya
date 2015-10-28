package mvm.rya.cloudbase;

import cloudbase.core.CBConstants;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;

/**
 * Interface CloudbaseRdfConstants
 * Date: Mar 1, 2012
 * Time: 7:24:52 PM
 */
public interface CloudbaseRdfConstants {
    public static final Authorizations ALL_AUTHORIZATIONS = CBConstants.NO_AUTHS;

    public static final Value EMPTY_VALUE = new Value(new byte[0]);

    public static final ColumnVisibility EMPTY_CV = new ColumnVisibility(new byte[0]);

}
