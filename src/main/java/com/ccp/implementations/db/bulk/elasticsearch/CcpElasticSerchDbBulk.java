package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;

public class CcpElasticSerchDbBulk implements CcpInstanceProvider<CcpDbBulkExecutor> {

	
	public CcpDbBulkExecutor getInstance() {
		ArrayList<CcpBulkItem> bulkItems = new ArrayList<>();
		return new ElasticSerchDbBulkExecutor(bulkItems);
	}

}
