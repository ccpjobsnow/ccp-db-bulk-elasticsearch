package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.db.bulk.CcpBulkAudit;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.db.utils.CcpOperationType;
import com.ccp.especifications.http.CcpHttpResponseType;

class DbBulkExecutorElasticSearch implements CcpDbBulkExecutor {
	private final Set<BulkItem> items = new HashSet<>();
	long lastUpdate = System.currentTimeMillis();

	
	@CcpDependencyInject
	private CcpDbUtils dbUtils;
	
	@Override
	public CcpMapDecorator commit(List<CcpMapDecorator> records, CcpOperationType operation, CcpEntity entity, CcpBulkAudit audit) {
		BulkOperation bulkOperation = BulkOperation.valueOf(operation.name());
		List<BulkItem> collect = records.stream().map(data -> new BulkItem(bulkOperation, data, entity)).collect(Collectors.toList());
		this.items.clear();
		this.items.addAll(collect);
		CcpMapDecorator bulkResult = this.execute();
		audit.commit(records,operation, bulkResult);
		return bulkResult;
	}

	private CcpMapDecorator execute() {
		
		if(this.items.isEmpty()) {
			return CcpConstants.EMPTY_JSON;
		}
		
		StringBuilder body = new StringBuilder();
		for (BulkItem bulkItem : this.items) {
			body.append(bulkItem.content);
		}
		this.items.clear();
		CcpMapDecorator headers = new CcpMapDecorator().put("Content-Type", "application/x-ndjson;charset=utf-8");
		CcpMapDecorator executeHttpRequest = this.dbUtils.executeHttpRequest("/_bulk", "POST", 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}
	
	
}
