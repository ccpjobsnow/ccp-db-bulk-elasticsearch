package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponseType;

class ElasticSerchDbBulkExecutor implements CcpDbBulkExecutor{
	
	private List<CcpBulkItem> bulkItems = new ArrayList<>();
	
	public CcpDbBulkExecutor addRecord(CcpBulkItem bulkItem) {
		this.bulkItems.add(bulkItem);
		return this;
	}

	public List<CcpBulkOperationResult> getBulkOperationResult() {
		if(this.bulkItems.isEmpty()) {
			return new ArrayList<>();
		}
		
		StringBuilder body = new StringBuilder();
		List<BulkItem> bulkItems = this.bulkItems.stream().map( x -> new BulkItem(x)).collect(Collectors.toList());
		for (BulkItem bulkItem : bulkItems) {
			body.append(bulkItem.content);
		}
		CcpJsonRepresentation headers = CcpConstants.EMPTY_JSON.put("Content-Type", "application/x-ndjson;charset=utf-8");
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("elasticSearchBulk", "/_bulk", "POST", 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> items = executeHttpRequest.getAsJsonList("items");

		List<CcpBulkOperationResult> collect = this.bulkItems.stream().map(bulkItem -> new ElasticSearchBulkOperationResult(bulkItem, items)).collect(Collectors.toList());
		this.bulkItems.clear();
		return collect;
	}

	public List<CcpBulkItem> getBulkItems() {
		return this.bulkItems;
	}

	
}
