package mvm.rya.cloudbase;

import cloudbase.core.security.Authorizations;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class CloudbaseRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    public static final String MAXRANGES_SCANNER = "cb.query.maxranges";

    public CloudbaseRdfConfiguration() {
        super();
    }

    public CloudbaseRdfConfiguration(Configuration other) {
        super(other);
    }

    @Override
    public CloudbaseRdfConfiguration clone() {
        return new CloudbaseRdfConfiguration(this);
    }

    public Authorizations getAuthorizations() {
        String[] auths = getAuths();
        if (auths == null || auths.length == 0)
            return CloudbaseRdfConstants.ALL_AUTHORIZATIONS;
        return new Authorizations(auths);
    }

    public void setMaxRangesForScanner(Integer max) {
        setInt(MAXRANGES_SCANNER, max);
    }

    public Integer getMaxRangesForScanner() {
        return getInt(MAXRANGES_SCANNER, 2);
    }
}
