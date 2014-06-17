package com.hp.msvua.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import backtype.storm.utils.Utils;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class Util {
	
	static Logger logger = Logger.getLogger(Util.class);
	
	public static final String PROP_FILE = "app.properties";
	
	public static Properties prop;
	
	static{
    	prop = new Properties();
    	InputStream in = Util.class.getClassLoader().getResourceAsStream(PROP_FILE);
    	try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

	public static String[] convertListToArr(List<Object> columnsList) {
		String[] columnsArr = null;
		if(columnsList != null && !columnsList.isEmpty()){
			columnsArr = new String[columnsList.size()];
			for (int i=0;i<columnsList.size();i++) {
				columnsArr[i] = (String)columnsList.get(i);
			}
		}
		return columnsArr;
	}
	
	public static ObjectId[] convertObjectIdListToArr(List<ObjectId> idList) {
		ObjectId[] columnsArr = null;
		if(idList != null && !idList.isEmpty()){
			columnsArr = new ObjectId[idList.size()];
			for (int i=0;i<idList.size();i++) {
				columnsArr[i] = (ObjectId)idList.get(i);
			}
		}
		return columnsArr;
	}
	
	public static Properties getConfigProperties(){
		if(prop==null){
			initProperties();
		}
		return prop;
	}

	private static void initProperties() {
		prop = new Properties();
		InputStream in = Util.class.getClassLoader().getResourceAsStream(PROP_FILE);
    	try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getConfigProperty(String key){
		return Util.getConfigProperties().getProperty(key);
	}

	public static List<ObjectId> convertToIdList(List<DBObject> aBatchRows) {
		List<ObjectId> idList = new ArrayList<>();
		for (DBObject obj : aBatchRows) {
			ObjectId _id = (ObjectId) obj.get("_id");
			idList.add(_id);
		}
		return idList;
	}

	public static String convertObjectIdArrToStr(ObjectId[] objectIds) {
		String result = "";
		if(objectIds!=null && objectIds.length>0){
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < objectIds.length; i++) {
				ObjectId objectId = objectIds[i];
				sb.append("{"+objectId+"},");
			}
			sb.deleteCharAt(sb.length()-1);
			result = sb.toString();
		}
		return result;
	}

	public static String convertDBObjectListToString(List<DBObject> batchRows) {
		String result = "";
		if(batchRows!=null && !batchRows.isEmpty()){
			StringBuffer sb = new StringBuffer();
			for (DBObject row : batchRows) {
				sb.append("{"+row.get("_id")+"},");
			}
			sb.deleteCharAt(sb.length()-1);
			result = sb.toString();
		}
		return result;
	}

	public static String convertObjectIdListToStr(List<ObjectId> rowIds) {
		ObjectId[] idArr = convertObjectIdListToArr(rowIds);
		return convertObjectIdArrToStr(idArr);
	}

	public static void sleepForawhile(long millionseconds) {
		logger.info("sleeping... idle time:"+millionseconds+"ms");
		Utils.sleep(millionseconds);
	}
	
	public static void sleepForawhile(String millionseconds){
		Long millionsecondsLong = Long.valueOf(millionseconds);
		if(StringUtils.isNotBlank(millionseconds) && millionsecondsLong instanceof Long){
			sleepForawhile(millionsecondsLong.longValue());
		} else{
			logger.error("millionseconds is blank or type is invalid...");
		}
	}

	public static String[] reorgenizeCollectionArr(String previousCollectionName, String[] collectionArr) {
		if(StringUtils.isBlank(previousCollectionName)){
			logger.debug("the first previousCollectionName is emtpy return the orginial array");
			return collectionArr; 
		}
		if(collectionArr==null || collectionArr.length==0){
			logger.error("collectionArr is empty...");
			return collectionArr;
		}
		int len = collectionArr.length;
		String[] result = new String[len];
		int index = 0;
		for (int i = 0; i < collectionArr.length; i++) {
			if(previousCollectionName.equals(collectionArr[i])){
				index = i;
				break;
			}
		}
		for (int i = 0; i < result.length; i++) {
			result[i] = collectionArr[(index+1)%len];
			index++;
		}
		return result;
	}
	
	public static void main(String[] args) {
		String[] arr = {"aa","bb","cc","dd"}; arr = new String[2];//{"a","b"};
		String preV = "dd";
		String[] result = Util.reorgenizeCollectionArr(preV, arr);
		for (int i = 0; i < result.length; i++) {
			System.out.println("name : "+result[i]);
		}
	}

	public static boolean shouldFilterField(String field, String[] filterArr) {
		boolean isFilterField = false;
		if(filterArr!=null && filterArr.length>0){
    		for (int i = 0; i < filterArr.length; i++) {
				if(field.equals(filterArr[i])){
					isFilterField = true;
					logger.debug("skip insert field["+field+"] to Vertica, it is a filter field...");
					break;
				}
			}
    	}
		return isFilterField;
	}

	public static String[] getCollectionField(DBObject row) {
		String[] result = new String[1];
		if(row!=null){
			Set<String> keySet = row.keySet();
			result = new String[keySet.size()];
			int index = 0;
			for (Iterator<String> it = keySet.iterator(); it.hasNext();) {
				String key = (String) it.next();
				result[index] = key;
				index++;
			}
		}
		return result;
	}

	public static void printRowIds(List<ObjectId> rowIds) {
		for (ObjectId objectId : rowIds) {
			System.out.println("_id:"+objectId);
		}
	}
	
	public static void printSelectedRows(List<DBObject> list) {
		if(list!=null && !list.isEmpty()){
			for (DBObject row : list) {
				System.out.println(row);
			}
		} else{
			System.out.println(" list is empty");
		}
	}
	
	public static void printCollection(DBCollection collection) {
		if(collection==null || collection.count()==0l){
			System.out.println("collection:"+collection+" is empty.");
			return;
		}
		DBCursor cursor = collection.find();
		for(;cursor.hasNext();){
			DBObject row = cursor.next();
			System.out.println(row);
		}
	}

	public static int retrieveUpdatedCount(int[] count) {
		if(count==null){
			return 0;
		}
		int c = 0;
		for (int i = 0; i < count.length; i++) {
			if(count[i]>0){
				c++;
			}
		}
		return c;
	}
}
