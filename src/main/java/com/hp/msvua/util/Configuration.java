package com.hp.msvua.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

public class Configuration {
	
	private static final String configPath = "/app.properties";
	private static Properties prop;
	
	static{
		/*try {
			InputStream fis = Object.class.getClass().getResourceAsStream(configPath);
			prop = new Properties();
			prop.load(fis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		try {
			initialize();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void initialize() throws IOException{
		InputStream fis = Object.class.getClass().getResourceAsStream(configPath);
		prop = new Properties();
		prop.load(fis);
	}
	
	public static String getValue(String key){
		if(prop==null){
			try {
				initialize();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		return prop.getProperty(key);
	}
	
	public static void main(String[] args) {
		String value = Configuration.getValue("mongodb.url2");
		System.out.println("value = "+(value==null?null:value));
	}

	public static int getValueAsInteger(String key) {
		Integer result = null;
		String value = getValue(key);
		if(value!=null){
			Integer tmp = Integer.valueOf(value);
			if(tmp instanceof Integer){
				result = tmp;
			}
		}
		return result;
	}
	
	public static long getValueAsLong(String key) {
		Long result = null;
		String value = getValue(key);
		if(value!=null){
			Long tmp = Long.valueOf(value);
			if(tmp instanceof Long){
				result = tmp;
			}
		}
		return result;
	}
	
	public static String[] getValueAsArray(String key){
		String[] result = null;
		if(StringUtils.isNotBlank(key)){
			String arrStr = getValue(key);
			if(StringUtils.isNotBlank(arrStr)){
				result = arrStr.split(",");
			}
		}
		return result;
	}
	

}
