package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.especifications.db.bulk.CcpBulkItem;

class BulkItem {
	final String id;
	final String entity;
	final String content;

	public BulkItem(CcpBulkItem item) {

		String name = item.operation.name();
		BulkOperation valueOf = BulkOperation.valueOf(name);
		String content = valueOf.getContent(item);
		this.entity = item.entity.getEntityName();
		this.content = content;
		this.id = item.id;
	}
	
	
	
	public String toString() {
		return "BulkItem [id=" + id + ", entity=" + entity + ", content=" + content + "]";
	}


	public int hashCode() {
		return (this.entity + this.id).hashCode();
	}
	
	
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			
			boolean differentEntity = other.entity.equals(this.entity) == false;
			
			if(differentEntity) {
				return false;
			}
			
			boolean differentId = other.id.equals(this.id) == false;
			
			if(differentId) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
