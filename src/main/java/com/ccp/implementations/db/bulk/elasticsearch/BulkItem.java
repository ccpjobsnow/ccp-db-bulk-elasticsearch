package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.db.bulk.CcpBulkOperation;

class BulkItem {
	final CcpBulkOperation operation;
	final CcpMapDecorator data;
	final String index;
	final String id;
	static final String NEW_LINE = System.getProperty("line.separator");
	final String content;
	public BulkItem(CcpBulkOperation operation, CcpMapDecorator data, String index, String id) {
		this.operation = operation;
		this.index = index;
		this.data = data;
		this.id = id;
		String string = "";
		string += new CcpMapDecorator().put(this.operation.name().toLowerCase(), new CcpMapDecorator().put("_index", this.index).put("_id", this.id)).asJson() + NEW_LINE;
		string += this.data.asJson() + NEW_LINE;
		this.content = new CcpStringDecorator(string).text().stripAccents();
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
