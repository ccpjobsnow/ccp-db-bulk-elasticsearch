package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpElasticSerchDbBulk implements CcpInstanceProvider {

	
	public Object getInstance() {
		return new ElasticSerchDbBulkExecutor();
	}

}
