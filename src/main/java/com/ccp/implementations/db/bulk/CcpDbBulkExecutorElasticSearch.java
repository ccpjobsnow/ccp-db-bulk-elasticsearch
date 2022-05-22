package com.ccp.implementations.db.bulk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.bulk.CcpBulkOperation;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpResponseType;

@CcpImplementation
public class CcpDbBulkExecutorElasticSearch implements CcpDbBulkExecutor {
	private final Set<BulkItem> items = new HashSet<>();
	long lastUpdate = System.currentTimeMillis();

	
	@CcpEspecification
	private CcpDbUtils dbUtils;
	
	@Override
	public CcpMapDecorator commit(List<CcpMapDecorator> records, CcpBulkOperation bulkOperation, String index) {
		List<BulkItem> collect = records.stream().map(x -> new BulkItem(bulkOperation, x, index, x.getAsString("id"))).collect(Collectors.toList());
		this.items.clear();
		this.items.addAll(collect);
		CcpMapDecorator execute = this.execute();
		return execute;
	}

	private CcpMapDecorator execute() {
		
		if(this.items.isEmpty()) {
			return CcpConstants.emptyJson;
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
