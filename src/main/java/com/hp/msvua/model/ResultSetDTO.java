package com.hp.msvua.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.DBObject;

public class ResultSetDTO implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private boolean isNotEmpty;
	
	private List<DBObject> batchRows;
	
	private String collectionName;
	
	private DBObject longestRow;
	
	public int getBatchRowsSize(){
		if(batchRows==null){
			batchRows = new ArrayList<>();
		}
		return batchRows.size();
	}
	

	public List<DBObject> getBatchRows() {
		return batchRows;
	}

	public void setBatchRows(List<DBObject> batchRows) {
		this.batchRows = batchRows;
	}

	public boolean isNotEmpty() {
		return isNotEmpty;
	}

	public void setNotEmpty(boolean isNotEmpty) {
		this.isNotEmpty = isNotEmpty;
	}


	public String getCollectionName() {
		return collectionName;
	}


	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}


	public DBObject getLongestRow() {
		return longestRow;
	}


	public void setLongestRow(DBObject longestRow) {
		this.longestRow = longestRow;
	}

}
