package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;

class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	private final Integer status;
	
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {

		String entityName = bulkItem.entity.getEntityName();
		String operationName = bulkItem.operation.name();

		CcpJsonRepresentation details = result.stream().map(x -> x.getInnerJson(operationName))
		.filter(x -> x.getAsString("_entity").equals(entityName))
		.filter(x -> x.getAsString("_id").equals(bulkItem.id))
		.findFirst().get();

		this.status = details.getAsIntegerNumber("status"); 
		this.errorDetails = details.getInnerJson("error");
		this.bulkItem = bulkItem;
	}
	
	public CcpJsonRepresentation getErrorDetails() {
		return errorDetails;
	}

	public CcpBulkItem getBulkItem() {
		return bulkItem;
	}

	public boolean isError() {
		boolean empty = this.errorDetails.isEmpty();
		return empty == false;
	}

	public int status() {
		return this.status;
	}

	
	
}
