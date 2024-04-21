package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.CcpEntity;

enum BulkOperation {
	delete {
		
		String getSecondLine(CcpJsonRepresentation data) {
			return "";
		}
	}, update {
		
		String getSecondLine(CcpJsonRepresentation data) {
			return CcpConstants.EMPTY_JSON.put("doc", data).asUgglyJson();
		}
	}, create {
		
		String getSecondLine(CcpJsonRepresentation data) {
			return data.asUgglyJson();
		}
	}
	;
	static final String NEW_LINE = System.getProperty("line.separator");

	public String getContent(CcpBulkItem item) {
//item.entity, item.json
		CcpJsonRepresentation values = item.entity.getOnlyExistingFields(item.json);
		
		String firstLine = this.getFirstLine(item.entity, values);
		
		String secondLine = this.getSecondLine(values);
		
		String content = firstLine + NEW_LINE + secondLine + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpEntity entity, CcpJsonRepresentation data) {
		String entityName = entity.getEntityName();
		String operationName = name();
		String id = entity.getId(data);
		String firstLine = CcpConstants.EMPTY_JSON
				.putSubKey(operationName, "_index", entityName)
				.putSubKey(operationName, "_id", id)
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation data);
	
}
