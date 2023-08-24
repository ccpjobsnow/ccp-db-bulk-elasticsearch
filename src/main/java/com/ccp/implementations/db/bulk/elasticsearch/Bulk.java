package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class Bulk implements CcpInstanceProvider {

	@Override
	public Object getInstance() {
		return new DbBulkExecutorElasticSearch();
	}

}
