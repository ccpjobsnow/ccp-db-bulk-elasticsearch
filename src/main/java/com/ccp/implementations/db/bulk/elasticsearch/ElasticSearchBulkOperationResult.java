package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;

class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {
		this.bulkItem = bulkItem;
		String operationName = bulkItem.operation.name();
		String entityName = bulkItem.entity.getEntityName();
		CcpJsonRepresentation errorDetails = result.stream().map(x -> x.getInnerJson(operationName))
		.filter(x -> x.getAsString("_entity").equals(entityName))
		.filter(x -> x.getAsString("_id").equals(bulkItem.id))
		.map(x -> x.getInnerJson("error"))
		.findFirst().get();
		this.errorDetails = errorDetails;
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

	@Override
	public int status() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
}
