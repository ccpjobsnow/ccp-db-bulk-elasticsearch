package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.especifications.db.bulk.CcpBulkable;
import com.ccp.especifications.db.utils.CcpDbTableField;

enum BulkOperation {
	delete {
		@Override
		String getSecondLine(CcpMapDecorator data) {
			return "";
		}
	}, update {
		@Override
		String getSecondLine(CcpMapDecorator data) {
			return new CcpMapDecorator().put("doc", data).asJson();
		}
	}, create {
		@Override
		String getSecondLine(CcpMapDecorator data) {
			return data.asJson();
		}
	}
	;
	static final String NEW_LINE = System.getProperty("line.separator");

	public String getContent(CcpBulkable bulkable, CcpMapDecorator data) {
		
		CcpDbTableField[] fields = bulkable.getFields();
		String[] array = Arrays.asList(fields).stream().map(x -> x.name()).collect(Collectors.toList()).toArray(new String[fields.length]);
		CcpMapDecorator values = data.getSubMap(array);
		String firstLine = this.getFirstLine(bulkable, values);
		
		String secondLine = this.getSecondLine(values);
		
		String content = firstLine + NEW_LINE + secondLine + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpBulkable bulkable, CcpMapDecorator data) {
		String indexName = bulkable.name();
		String operationName = name();
		String id = bulkable.getId(data);
		String firstLine = new CcpMapDecorator()
				.putSubKey(operationName, "_index", indexName)
				.putSubKey(operationName, "_id", id)
				.asJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpMapDecorator data);
	
}
