
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.bulk.CcpBulkItem;

enum BulkOperation {
	delete {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	}, update {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return CcpOtherConstants.EMPTY_JSON.put("doc", json).asUgglyJson();
		}
	}, create {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return json.asUgglyJson();
		}
	}
	;
	static final String NEW_LINE = System.getProperty("line.separator");

	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);
		
		String secondLine = this.getSecondLine(item.json);
		
		String content = firstLine + NEW_LINE + secondLine + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpBulkItem item) {
		String entityName = item.entity.getEntityName();
		String operationName = this.name();
		String firstLine = CcpOtherConstants.EMPTY_JSON
				.addToItem(operationName, "_index", entityName)
				.addToItem(operationName, "_id", item.id)
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation json);
	
}
