package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpResponseType;

public class ElasticSerchDbBulkExecutor implements CcpDbBulkExecutor{
	
	private List<CcpBulkItem> bulkItems = new ArrayList<>();
	
	public CcpDbBulkExecutor addRecord(CcpBulkItem bulkItem) {
		this.bulkItems.add(bulkItem);
		return this;
	}

	public List<CcpJsonRepresentation> getFailedRecords(List<CcpJsonRepresentation> auditRecords) {
		List<CcpJsonRepresentation> collect = auditRecords.stream().filter(item -> item.containsKey("error")).collect(Collectors.toList());
		return collect;
	}

	public List<CcpJsonRepresentation> getSuccedRecords(List<CcpJsonRepresentation> auditRecords) {
		List<CcpJsonRepresentation> collect = auditRecords.stream()
				.filter(item -> item.containsKey("error") == false)
				.filter(item -> item.getAsBoolean("auditable"))
				.collect(Collectors.toList());
		return collect;
	}

	public CcpJsonRepresentation getAuditRecord(CcpBulkItem bulkItem, CcpJsonRepresentation bulkResult) {
		Integer status = bulkResult.getAsIntegerNumber("status");
		
		CcpJsonRepresentation recordFound = bulkResult.getAsJsonList("items").stream().filter(item -> item.getAsString("_id").equals(bulkItem.id)).findFirst().orElseThrow(() -> new RuntimeException("" + bulkItem + " could not be found"));
		CcpJsonRepresentation result = recordFound.getInnerJson(bulkItem.operation.name());
		CcpJsonRepresentation errorDetails = result.getInnerJson("error").renameKey("type", "errorType").getJsonPiece("errorType", "reason");
		boolean auditable = bulkItem.entity.isAuditable();
		CcpJsonRepresentation json = bulkItem.getJson();
		CcpJsonRepresentation auditRecord = CcpConstants.EMPTY_JSON
				.put("date", System.currentTimeMillis())
				.put("auditable", auditable)
				.put("status", status)
				.putAll(errorDetails)
				.putAll(json)
				;
		return auditRecord;
	}

	public List<CcpBulkItem> getBulkItems() {
		return this.bulkItems;
	}

	public CcpJsonRepresentation getBulkResult() {
		if(this.bulkItems.isEmpty()) {
			return CcpConstants.EMPTY_JSON;
		}
		
		StringBuilder body = new StringBuilder();
		List<BulkItem> bulkItems = this.bulkItems.stream().map( x -> new BulkItem(x)).collect(Collectors.toList());
		for (BulkItem bulkItem : bulkItems) {
			body.append(bulkItem.content);
		}
		this.bulkItems.clear();
		CcpJsonRepresentation headers = CcpConstants.EMPTY_JSON.put("Content-Type", "application/x-ndjson;charset=utf-8");
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("elasticSearchBulk", "/_bulk", "POST", 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}

}
