package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.utils.CcpEntity;

class BulkItem {
	final String id;
	final String entity;
	final String content;

	public BulkItem(BulkOperation operation, CcpJsonRepresentation data, CcpEntity entity) {

		this.content = operation.getContent(entity, data);
		this.id = entity.getId(data);
		this.entity = entity.name();
		
		
	}
	
	
	public int hashCode() {
		return (this.entity + this.id).hashCode();
	}
	
	
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			if(other.entity.equals(this.entity) == false) {
				return false;
			}
			if(other.id.equals(this.id) == false) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
