package mvm.rya.api.query.strategy.wholerow;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;

public class MockRdfCloudConfiguration extends RdfCloudTripleStoreConfiguration {

	@Override
	public RdfCloudTripleStoreConfiguration clone() {
		return this;
	}

}
