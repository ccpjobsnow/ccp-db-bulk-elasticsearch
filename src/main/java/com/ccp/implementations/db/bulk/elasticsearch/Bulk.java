package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.dependency.injection.CcpModuleExporter;

public class Bulk implements CcpModuleExporter {

	@Override
	public Object export() {
		return new DbBulkExecutorElasticSearch();
	}

}
