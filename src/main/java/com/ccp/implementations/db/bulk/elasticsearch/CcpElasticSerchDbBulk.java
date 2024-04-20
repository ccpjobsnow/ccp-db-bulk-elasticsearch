package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;

public class CcpElasticSerchDbBulk implements CcpInstanceProvider<CcpDbBulkExecutor> {

	
	public CcpDbBulkExecutor getInstance() {
		return new ElasticSerchDbBulkExecutor();
	}

}
