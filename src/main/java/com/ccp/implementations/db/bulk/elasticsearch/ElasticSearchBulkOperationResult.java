package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.utils.CcpDbRequester;

class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	private final Integer status;
	
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {

		String entityName = bulkItem.entity.getEntityName();
		String operationName = bulkItem.operation.name();
		CcpDbRequester dependency = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String fieldNameToEntity = dependency.getFieldNameToEntity();
		String fieldNameToId = dependency.getFieldNameToId();
		List<CcpJsonRepresentation> map = result.stream().map(x -> x.getInnerJson(operationName)).collect(Collectors.toList());
		
		List<CcpJsonRepresentation> filteredById = map.stream().filter(x -> x.getAsString(fieldNameToId).equals(bulkItem.id)).collect(Collectors.toList());
		
		if(filteredById.isEmpty()) {
			String format = String.format( "Id '%s' not found. Complete list: " + result, bulkItem.id, entityName);
			throw new RuntimeException(format);
		}
		Optional<CcpJsonRepresentation> findFirst = filteredById.stream()
		.filter(x -> x.getAsString(fieldNameToEntity).equals(entityName))
		.findFirst();
		
		boolean idNotFoundInTheEntity = findFirst.isPresent() == false;
		
		if(idNotFoundInTheEntity) {
			String format = String.format( "Id '%s' not found in the entity '%s' Complete list: " + result, bulkItem.id, entityName);
			throw new RuntimeException(format);
		}
		
		CcpJsonRepresentation details = findFirst.get();

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

	public boolean hasError() {
		boolean empty = this.errorDetails.isEmpty();
		return empty == false;
	}

	public int status() {
		return this.status;
	}

	
	public String toString() {
		CcpJsonRepresentation asMap = this.asMap();
		String string = asMap.toString();
		return string;
	}

	public CcpJsonRepresentation asMap() {
		CcpJsonRepresentation asMap = this.bulkItem.asMap();
		CcpJsonRepresentation put = CcpConstants.EMPTY_JSON
				.put("errorDetails", this.errorDetails)
				.put("status", this.status)
				.put("bulkItem", asMap)
				;
		return put;
	}
}
