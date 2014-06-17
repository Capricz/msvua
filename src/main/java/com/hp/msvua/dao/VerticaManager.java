package com.hp.msvua.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;

import com.hp.msvua.util.Configuration;
import com.hp.msvua.util.Const;
import com.hp.msvua.util.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class VerticaManager implements Serializable {
	
	private static final long serialVersionUID = 1L;

	static Logger logger = Logger.getLogger(VerticaManager.class);
	
	Connection conn;
    PreparedStatement pstmt;
    String[] keywords;
    String[] filterArr;
    DateFormat sdf = new SimpleDateFormat(Const.DATE_FORMAT);

    
	
	public VerticaManager(){
		initialize();
	}

	public void initialize() {
		String url = Configuration.getValue(Const.VERTICA_URL);
		String username = Configuration.getValue(Const.VERTICA_USERNAME);
		String password = Configuration.getValue(Const.VERTICA_PASSWORD);
		keywords = Configuration.getValueAsArray(Const.VERTICA_KEYWORDS);
		filterArr = Configuration.getValueAsArray(Const.VERTICA_FILTER_FIELDS);
		try {
			conn = DriverManager.getConnection(url, username, password);
//			((VerticaConnection) conn).setProperty("DirectBatchInsert", true);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void generatePreparedStatement(String collectionName, DBObject row) throws SQLException,Exception {
		if(conn==null){
			initialize();
		}
		String[] fields = Util.getCollectionField(row);
		if(filterArr!=null && fields.length == filterArr.length){
			throw new Exception("error:column size equals to filter name list size");
		}
		logger.debug("Now inserting the rows to vertica!!!!!!!");

        String sql = " INSERT INTO " + collectionName + " (";
        StringBuffer sbColumn = new StringBuffer();
        StringBuffer sbValues = new StringBuffer();
        
        boolean isLastFilterField = false;
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
//                Object object = inputs.getValueByField(field);
            //for filter '-', e.g. tracking-id -> trackingId
            field = field.replaceAll("-|_", "");
//            logger.debug("field name : "+field);
            //filter column by filter Array, check if the current one is last one
            if(i != fields.length - 1 && Util.shouldFilterField(field,filterArr)){
            	logger.debug("field name : "+field+" in the mid columns, i : "+i+", skip for filter...");
            	continue;
        	} else if(Util.shouldFilterField(field,filterArr)){
        		isLastFilterField = true;
        		logger.debug("field name : "+field+" last one is filter field, mark filterField is true, i : "+i);
        	} else{
        		logger.debug("field name : "+field+" normal field, name:"+field+", i:"+i);
        	}

            //add '\' for the field if it is a keyword in vertica
            if (Arrays.asList(keywords).contains(field)) {
                sbColumn.append("\"" + field + "\"" + ",");
            } else {
                sbColumn.append(field + ",");
            }

            //remove last comma,append sql for start of values
            if (i == fields.length - 1) {
                sbColumn.deleteCharAt(sbColumn.length() - 1);
//                logger.debug("after remove last ',' sql:"+sbColumn.toString());
                //remove last one, since it is in filter name list
                if(isLastFilterField){
                	sbColumn.delete(sbColumn.lastIndexOf(","), sbColumn.length());
//                	logger.debug("filter last column, sql:"+sbColumn.toString());
                }
                sbColumn.append(") VALUES(");
            }

            sbValues.append("?,");

            //remove last comma,append sql for end of values
            if (i == fields.length - 1) {
                sbValues.deleteCharAt(sbValues.length() - 1);
              //remove last one, since it is in filter name list
                if(isLastFilterField){
                	logger.debug("filter last '?', sql:"+sbValues.toString());
                	sbValues.delete(sbValues.lastIndexOf(","), sbValues.length());
                }
                sbValues.append(")");
            }
        }

        sql += sbColumn.toString() + sbValues;

        logger.debug("insert table: " + collectionName + ", sql : " + sql);

        //insert paramenter
		pstmt = conn.prepareStatement(sql);
	}

	public void addRowToBatch(DBObject row, DBObject longestRow) throws SQLException {
		Set<String> keySet = row.keySet();
		int index = 0;
		for (String key : longestRow.keySet()) {
        	Object object = row.get(key);
        	String field = key.replaceAll("-|_", "");
        	if(null!=object){
        		logger.debug("the column from mongo field : "+field+", key : "+key+", value[Class: " + object.getClass()+"] : "+object.toString());
        	}else{
        		logger.debug("the column from mongo field : "+field+", key : "+key);
        	}
        	
        	if(Util.shouldFilterField(field,filterArr)){
        		continue;
        	}
        	
        	index++;
//                logger.debug("set pstmt field :" + field+ "[index:"+index+"]);
	        	if (null == object) {
	            	logger.warn("field : "+field+" data not found...");
	                pstmt.setString(index, "");
	            } else if (object instanceof ObjectId) {
                    ObjectId objectId = (ObjectId) object;
//                    logger.debug("ObjectId value : " + objectId);
                    pstmt.setString(index, objectId.toString());
                } else if (object instanceof Date) {
                    Date date = (Date) object;
//                    logger.debug("Date value : " + date);
//					pstmt.setString(index, sdf .format(date));
                    pstmt.setTimestamp(index, new Timestamp(date.getTime()));
                } else if (object instanceof Timestamp) {
                    Timestamp time = (Timestamp) object;
//                    logger.debug("Timestamp value : " + time);
                    pstmt.setTimestamp(index, time);
                } else if (object instanceof BSONTimestamp) {
                    BSONTimestamp time = (BSONTimestamp) object;
//                    logger.debug("BSONTimestamp value : " + time);
                    pstmt.setTimestamp(index, new Timestamp(time.getTime()));
                } else if( object instanceof Integer){
                	Integer num = (Integer) object;
                	if(Const.FIELD_OFFSET.equals(field)){
                		pstmt.setString(index, String.valueOf(num.intValue()));
                	} else{
                		pstmt.setInt(index, num);
                	}
                }else if( object instanceof Double){
                	Double num = (Double) object;
                	if(Const.FIELD_OFFSET.equals(field)){
                		pstmt.setString(index, String.valueOf(num.intValue()));
                	} else{
                		pstmt.setDouble(index, num);
                	}
                }else if (object instanceof String) {
                    String value = (String) object;
//                    value = value.replaceAll(";","\\;");
//                    logger.debug("String value : " + value);
                    pstmt.setString(index, value);
                } else if (object instanceof BasicDBObject) {
                    BasicDBObject dbObj = (BasicDBObject) object;
//					Set<Entry<String,Object>> entrySet = dbObj.entrySet();
//                    logger.debug("BasicDBObject value : " + dbObj);
                    pstmt.setString(index, dbObj.toString());
                } else {
                	logger.info("type not found, field : "+field+", Class : "+object.getClass());
                    String value = object.toString();
                    pstmt.setString(index, value);
                }
            }
            pstmt.addBatch();
    }

	public int[] executeBatchRows() throws Exception {
		logger.debug("pending execute inserting...");
		if(pstmt==null){
			throw new Exception("pstmt is null");
		}
        
		int[] count;
		try {
			count = pstmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
        logger.info("insert complete successfully, count : " + Util.retrieveUpdatedCount(count));
        return count;
	}
	
	public static void main(String[] args) {
		VerticaManager manager = new VerticaManager();
		
		DBObject row = new BasicDBObject();
		row.put("id", "123");
		row.put("group", "group1");
		row.put("name", "panasonic");
		row.put("time", new Timestamp((new Date()).getTime()));
		row.put("flag", 0);
		try {
			logger.debug("insert row : "+row);
			manager.addRowToBatch(row,row);
			manager.executeBatchRows();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		logger.info("in VerticaManager - close vertica connection...");
		if(pstmt!=null){
			try {
				pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
	}

	public boolean isExistedInVertica(String tableName,DBObject row) {
		String sql = " SELECT COUNT(*) FROM "+tableName+" WHERE "+Const.FIELD_ID + " =?";
		int count = 0;
		try {
			pstmt.setString(1, (String)row.get(Const.FIELD_ID));
			ResultSet rs = pstmt.executeQuery();
			count = rs.getInt(0);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count>0?true:false;
	}
}