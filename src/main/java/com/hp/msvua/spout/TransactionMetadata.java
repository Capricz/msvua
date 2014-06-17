package com.hp.msvua.spout;

import java.io.Serializable;
import java.util.List;

import org.bson.types.ObjectId;

public class TransactionMetadata implements Serializable {

	private static final long serialVersionUID = 1L;
	
//	long from;
//	int quantity;
	List<ObjectId> rowIds;
	String collectionName;
	
	/*public TransactionMetadata(long from,int quantity){
		this.from = from;
		this.quantity = quantity;
	}*/

	public TransactionMetadata(String collectionName,List<ObjectId> rowIds) {
		this.rowIds = rowIds;
		this.collectionName = collectionName;
	}

}
