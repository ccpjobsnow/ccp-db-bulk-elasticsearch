package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.db.utils.CcpEntityOperationType;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.bulk.CcpBulkErrors;

class ElasticSerchDbBulkExecutor implements CcpDbBulkExecutor {
	private final Set<BulkItem> items = new HashSet<>();
	long lastUpdate = System.currentTimeMillis();

	

	public void audit(CcpEntity entity, CcpEntity auditEntity, CcpJsonRepresentation errorsAndSuccess,  CcpEntityOperationType operation) {

		boolean isNotAuditable = entity.isAuditable() == false;
		
		if(isNotAuditable) {
			return;
		}
		
		List<CcpJsonRepresentation> succedRecords = errorsAndSuccess.getAsJsonList("succedRecords");
		this.commit(succedRecords, operation, auditEntity);
	}



	public void saveErrors(CcpEntity entity, CcpEntity errorEntity, CcpJsonRepresentation errorsAndSuccess,  CcpEntityOperationType operation) {

		List<CcpJsonRepresentation> failedRecords = errorsAndSuccess.getAsJsonList("failedRecords");
		
		boolean hasNoErrors = failedRecords.isEmpty();
		
		if(hasNoErrors) {
			return;
		}
		
		this.commit(failedRecords, operation, errorEntity);
		
		throw new CcpBulkErrors(failedRecords);
	}

	
	private CcpJsonRepresentation getAuditObject(CcpEntity entity, List<CcpJsonRepresentation> records, CcpJsonRepresentation error, CcpEntityOperationType operation) {
		String id = error.getAsString("_id");
		String entityName = error.getAsString("_index");
		Integer status = error.getAsIntegerNumber("status");
		CcpJsonRepresentation errorDetails = error.getInnerJson("error").renameKey("type", "errorType").getJsonPiece("errorType", "reason");
		
		CcpJsonRepresentation json = new ArrayList<>(records).stream().filter(record -> this.filter(entity, id, record)).findFirst().get();
		CcpJsonRepresentation mappedError = CcpConstants.EMPTY_JSON.put("date", System.currentTimeMillis()).put("operation", operation.name())
				.put("entity", entityName).put("id", id).put("json", json).put("status", status).putAll(errorDetails)
				;
		return mappedError;
	}



	private boolean filter(CcpEntity entity, String id, CcpJsonRepresentation record) {
		String id2 = entity.getId(record);
		return id2.equals(id);
	}
	
	
	public CcpJsonRepresentation commit(List<CcpJsonRepresentation> records, CcpEntityOperationType operation, CcpEntity entity) {
		if(records.isEmpty()) {
			return CcpConstants.EMPTY_JSON;
		}
		
		BulkOperation bulkOperation = BulkOperation.valueOf(operation.name());
		List<BulkItem> collect = records.stream().map(data -> new BulkItem(bulkOperation, data, entity)).collect(Collectors.toList());
		this.items.clear();
		this.items.addAll(collect);
		CcpJsonRepresentation bulkResult = this.execute();
		
		CcpJsonRepresentation result = CcpConstants.EMPTY_JSON;

		List<CcpJsonRepresentation> failedRecords = new ArrayList<>();
		List<CcpJsonRepresentation> succedRecords = new ArrayList<>();

		List<CcpJsonRepresentation> items = bulkResult.getAsJsonList("items").stream()
				.map(x -> x.getInnerJson(operation.name())).collect(Collectors.toList());

		for (CcpJsonRepresentation item : items) {

			CcpJsonRepresentation auditObject = this.getAuditObject(entity, records, item, operation);

			boolean hasNoError = item.containsKey("error") == false;

			if (hasNoError) {
				succedRecords.add(auditObject);
				continue;
			}

			failedRecords.add(auditObject);
		}
		result = result.put("failedRecords", failedRecords).put("succedRecords", succedRecords);

		return result;
	}

	private CcpJsonRepresentation execute() {
		
		if(this.items.isEmpty()) {
			return CcpConstants.EMPTY_JSON;
		}
		
		StringBuilder body = new StringBuilder();
		for (BulkItem bulkItem : this.items) {
			body.append(bulkItem.content);
		}
		this.items.clear();
		CcpJsonRepresentation headers = CcpConstants.EMPTY_JSON.put("Content-Type", "application/x-ndjson;charset=utf-8");
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("/_bulk", "POST", 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}


	
	
}
