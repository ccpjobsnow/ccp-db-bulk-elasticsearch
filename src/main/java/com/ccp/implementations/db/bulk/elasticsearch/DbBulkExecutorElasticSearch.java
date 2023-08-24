package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpInstanceInjection;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.db.utils.CcpOperationType;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.bulk.BulkErrors;

class DbBulkExecutorElasticSearch implements CcpDbBulkExecutor {
	private final Set<BulkItem> items = new HashSet<>();
	long lastUpdate = System.currentTimeMillis();

	

	public void audit(CcpEntity entity, CcpEntity auditEntity, CcpMapDecorator errorsAndSuccess,  CcpOperationType operation) {

		boolean isNotAuditable = entity.isAuditable() == false;
		
		if(isNotAuditable) {
			return;
		}
		
		List<CcpMapDecorator> succedRecords = errorsAndSuccess.getAsMapList("succedRecords");
		this.commit(succedRecords, operation, auditEntity);
	}



	public void saveErrors(CcpEntity entity, CcpEntity errorEntity, CcpMapDecorator errorsAndSuccess,  CcpOperationType operation) {

		List<CcpMapDecorator> failedRecords = errorsAndSuccess.getAsMapList("failedRecords");
		
		boolean hasNoErrors = failedRecords.isEmpty();
		
		if(hasNoErrors) {
			return;
		}
		
		this.commit(failedRecords, operation, errorEntity);
		
		throw new BulkErrors(failedRecords);
	}

	
	private CcpMapDecorator getAuditObject(CcpEntity entity, List<CcpMapDecorator> records, CcpMapDecorator error, CcpOperationType operation) {
		String id = error.getAsString("_id");
		String entityName = error.getAsString("_index");
		Integer status = error.getAsIntegerNumber("status");
		CcpMapDecorator errorDetails = error.getInternalMap("error").renameKey("type", "errorType").getSubMap("errorType", "reason");
		
		CcpMapDecorator json = new ArrayList<>(records).stream().filter(record -> this.filter(entity, id, record)).findFirst().get();
		CcpMapDecorator mappedError = new CcpMapDecorator().put("date", System.currentTimeMillis()).put("operation", operation.name())
				.put("entity", entityName).put("id", id).put("json", json).put("status", status).putAll(errorDetails)
				;
		return mappedError;
	}



	private boolean filter(CcpEntity entity, String id, CcpMapDecorator record) {
		String id2 = entity.getId(record);
		return id2.equals(id);
	}
	
	@Override
	public CcpMapDecorator commit(List<CcpMapDecorator> records, CcpOperationType operation, CcpEntity entity) {
		if(records.isEmpty()) {
			return new CcpMapDecorator();
		}
		
		BulkOperation bulkOperation = BulkOperation.valueOf(operation.name());
		List<BulkItem> collect = records.stream().map(data -> new BulkItem(bulkOperation, data, entity)).collect(Collectors.toList());
		this.items.clear();
		this.items.addAll(collect);
		CcpMapDecorator bulkResult = this.execute();
		
		CcpMapDecorator result = new CcpMapDecorator();

		List<CcpMapDecorator> failedRecords = new ArrayList<>();
		List<CcpMapDecorator> succedRecords = new ArrayList<>();

		List<CcpMapDecorator> items = bulkResult.getAsMapList("items").stream()
				.map(x -> x.getInternalMap(operation.name())).collect(Collectors.toList());

		for (CcpMapDecorator item : items) {

			CcpMapDecorator auditObject = this.getAuditObject(entity, records, item, operation);

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
		CcpDbUtils dbUtils = CcpInstanceInjection.getInstance(CcpDbUtils.class);
		CcpMapDecorator executeHttpRequest = dbUtils.executeHttpRequest("/_bulk", "POST", 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		return executeHttpRequest;
	}


	
	
}
