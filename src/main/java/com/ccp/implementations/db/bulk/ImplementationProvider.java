package com.ccp.implementations.db.bulk;

import com.ccp.dependency.injection.CcpImplementationProvider;

public class ImplementationProvider implements CcpImplementationProvider {

	@Override
	public Object getImplementation() {
		return new DbBulkExecutorElasticSearch();
	}

}
