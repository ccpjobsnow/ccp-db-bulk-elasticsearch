package com.ccp.implementations.db.bulk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;
import com.ccp.especifications.db.bulk.CcpBulkOperation;
import com.ccp.especifications.db.credentials.generator.CcpDbCredentialsGenerator;
import com.ccp.especifications.http.CcpHttp;
import com.ccp.especifications.http.CcpHttpHandler;

@CcpImplementation
public class CcpBulkElasticSearch implements CcpBulkExecutor {
	private final Set<BulkItem> items = new HashSet<>();
	long lastUpdate = System.currentTimeMillis();
	public final long maxTimeInMiliSeconds = Long.valueOf(System.getenv("BULK_MAX_TIME"));
	public final int maxSize = Integer.valueOf(System.getenv("BULK_MAX_SYZE"));

	
	@CcpEspecification
	private CcpHttp ccpHttp;
	
	@CcpEspecification
	private CcpDbCredentialsGenerator dbCredentials;
	
	@Override
	public CcpMapDecorator commit(List<CcpMapDecorator> records, CcpBulkOperation bulkOperation, String index) {
		List<BulkItem> collect = records.stream().map(x -> new BulkItem(bulkOperation, x, index, x.getAsString("id"))).collect(Collectors.toList());
		this.items.clear();
		this.items.addAll(collect);
		CcpMapDecorator execute = this.execute();
		return execute;
	}

	@Override
	public CcpMapDecorator append(CcpBulkOperation operation, CcpMapDecorator data, String index, String id) {
		synchronized(String.class) {
			this.items.add(new BulkItem(operation, data, index, id));
			
			boolean itIsTimeToExecute = this.itIsTimeToExecute();
			
			if(itIsTimeToExecute) {
				return this.execute();
			}
			
			boolean itIsSizeToExecute = this.itIsSizeToExecute();
			
			if(itIsSizeToExecute) {
				return this.execute();
			}
			
			return new CcpMapDecorator();
			
		}
	}

	private CcpMapDecorator execute() {
		
		if(this.items.isEmpty()) {
			return new CcpMapDecorator();
		}
		
		StringBuilder body = new StringBuilder();
		CcpMapDecorator headers = this.dbCredentials.getDatabaseCredentials();
		String url = headers.getAsString("DB_URL");
		for (BulkItem bulkItem : this.items) {
			body.append(bulkItem.content);
		}

		headers = headers.put("Content-Type", "application/x-ndjson;charset=utf-8");

		CcpHttpHandler http = new CcpHttpHandler(200, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, body.toString());
		this.items.clear();
		return executeHttpRequest;
	}

	private boolean itIsSizeToExecute() {
		boolean itIsSizeToExecute = this.items.size() >= this.maxSize;
		return itIsSizeToExecute;
	}

	private boolean itIsTimeToExecute() {
		long now = System.currentTimeMillis();
		long enlapsedTime = now - this.lastUpdate;
		boolean itIsTimeToExecute = enlapsedTime > this.maxTimeInMiliSeconds;
		return itIsTimeToExecute;
	}

}
