
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.CcpEntity;

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
//item.entity, item.json
		CcpJsonRepresentation json = item.entity.getOnlyExistingFieldsAndHandledJson(item.json);
		
		String firstLine = this.getFirstLine(item.entity, json);
		
		String secondLine = this.getSecondLine(json);
		
		String content = firstLine + NEW_LINE + secondLine + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpEntity entity, CcpJsonRepresentation json) {
		String entityName = entity.getEntityName();
		String operationName = name();
		String id = entity.calculateId(json);
		String firstLine = CcpOtherConstants.EMPTY_JSON
				.addToItem(operationName, "_index", entityName)
				.addToItem(operationName, "_id", id)
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation json);
	
}
