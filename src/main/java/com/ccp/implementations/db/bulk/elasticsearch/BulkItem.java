package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.especifications.db.bulk.CcpBulkable;

class BulkItem {
	final String id;
	final String index;
	final String content;

	public BulkItem(BulkOperation operation, CcpMapDecorator data, CcpBulkable index) {

		this.content = operation.getContent(index, data);
		this.id = index.getId(data);
		this.index = index.name();
		
		
	}
	
	@Override
	public int hashCode() {
		return (this.index + this.id).hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			if(other.index.equals(this.index) == false) {
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
