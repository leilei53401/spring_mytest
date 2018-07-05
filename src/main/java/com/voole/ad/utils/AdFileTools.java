package com.voole.ad.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 广告工具类
 * @author Administrator
 *
 */
public class AdFileTools {
	
	private static Logger logger = LoggerFactory.getLogger(Class.class);
	
	/**
	 * 单条写文件
	 * @param filePath
	 * @param fileName
	 * @param hidLine
	 * @param appead
	 */
	public static void writeLineToFile(String filePath, String fileName, String line, boolean appead){
		File file = FileUtils.getFile(new File(filePath),fileName);
		try {
			FileUtils.writeStringToFile(file, line, appead);
		} catch (IOException e) {
			logger.error("AdTools  writeLineToFile to file exception!",e);
		}
	}
	/**
	 * 单条写文件
	 * @param filePath
	 * @param fileName
	 * @param hidLine
	 * @param appead
	 */
	public static void writeLineToFile(String filePath, String line, boolean appead){
		File file = FileUtils.getFile(new File(filePath));
		try {
			FileUtils.writeStringToFile(file, line, appead);
		} catch (IOException e) {
			logger.error("AdTools  writeLineToFile to file exception!",e);
		}
	}
	/**
	 * 批量写文件
	 * @param filePath
	 * @param fileName
	 * @param hidLine
	 * @param lines 
	 */
	public static void writeBatchToFile(String filePath, String fileName, Collection<?> lines, boolean appead){
		File file = FileUtils.getFile(new File(filePath),fileName);
		try {
			FileUtils.writeLines(file, lines, appead);
		} catch (IOException e) {
			logger.error("AdDataServiceImpl  writeHidToFile to file exception!",e);
		}
	}
	
	/**
	 * 读取文件(一次性读入内存)
	 * @param filePath
	 * @param fileName
	 * @return
	 */
	public static List<String> readHidFile(String filePath, String fileName){
		List<String> readLines = null;
		try {
			readLines = FileUtils.readLines(new File(filePath + "/" + fileName), "UTF-8");
		} catch (IOException e) {
			logger.error("AdTools readHidFile  to cache exception!",e);
		}
		return readLines;
	}
	
	/**
	 * 读取文件（一次性读入）
	 * @param file
	 * @return
	 */
	public static List<String> readlinesFile(File file){
		List<String> readLines = null;
		try {
			readLines = FileUtils.readLines(file, "UTF-8");
		} catch (IOException e) {
			logger.error("AdTools readHidFile  to cache exception!",e);
		}
		return readLines;
	}
	
	/**
	 * 行读取
	 * @param file
	 * @return
	 */
	public static LineIterator  readIteratorFile(File file){
		LineIterator iterator = null;
		try {
			iterator = FileUtils.lineIterator(file, "UTF-8");
		} catch (IOException e) {
			logger.error("AdTools readIteratorFile  to cache exception!",e);
		}
		return iterator;
	}
	
	/**
	 * 删除文件
	 * @param filePath
	 */
	public static void delFile(String filePath){
		try {
			FileUtils.forceDelete(new File(filePath));
		} catch (IOException e) {
			logger.error("AdTools  delFile to file exception!",e);
		}
	}
	
	
	/**
	 * 新建文件
	 * @param file
	 */
	public static void newFile(String file){
		try {
			FileUtils.forceMkdir(new File(file));
		} catch (IOException e) {
			logger.error("AdTools newFile to file exception!",e);
		}
	}
	/**
	 * 十进制ip转换为点分式ip
	 * @param ipDec
	 * @return
	 */
	public static  String decimalToIp(long ipDec) {
        return  new StringBuilder().append(((ipDec >> 24) & 0xff)).append('.')
                .append((ipDec >> 16) & 0xff).append('.').append(
                        (ipDec >> 8) & 0xff).append('.').append((ipDec & 0xff))
                .toString();
    }
	
	/**
	 *ip转换成十进制的数
	 *ip格式：*.*.*.*
	 * @param ip
	 * @return
	 */
	public static long ipToDecimal(String ip){
		long ipDec = 0;
		if(ip != null){
			String[] ipArr = ip.split("\\.");
			if (ipArr != null && ipArr.length == 4){
				for (int i = 3; i >= 0; i--) {
					ipDec += (Long.valueOf(ipArr[i]) * Math.pow(256, 3-i));
				}
			}
		}
		return ipDec;
	}
	
	
	/**
	 * object转换为整形
	 * @param o
	 * @return
	 */
	public static Integer ObjectToInt(Object o){
		Integer result = null;
		if(o != null){
			String num = o.toString();
			if(num.indexOf("-")<0 && StringUtils.isNumeric(num)){
				result = Integer.valueOf(num);
			}else if(num.length() > 0 & StringUtils.isNumeric(num.substring(1))){
				result = Integer.valueOf(num);
			}
		}
		return result;
	}
	
	/**
	 * 从map里取String  为空时返回空串
	 * @param map
	 * @param key
	 * @return
	 */
	public static String getValueMap2Str(Map<String,Object> map,String key){
		String value = "";
		if(map != null  && map.size()>0 && key != null){
			Object valueObj = map.get(key);
			if(valueObj != null){
				value = valueObj.toString();
			}
		}
		return value;
	}
	
	/**
	 * 生成文件索引的bitset
	 * @param file
	 * @return
	 */
	public static BitSet fileIndexBit(File file){
		BitSet bitSet = new BitSet();
		LineIterator iterator = readIteratorFile(file);
		int index = 0;
		while(iterator.hasNext()){
			if(StringUtils.isNotBlank(iterator.next())){
				bitSet.set(++index);
			}
		}
		return bitSet;
	}
	
	/**
	 * 获取{@bitset}中获取有效值，并随机查收{@count}个
	 * @param bitSet
	 * @param count
	 * @return
	 */
	public static List<Integer> randomBitSet(BitSet bitSet, int count){
		List<Integer> list = new ArrayList<Integer>();
		int length = bitSet != null ? bitSet.length() : 0;
		for(int i=0; i<length; i++){
			if(bitSet.get(i)){
				list.add(i);
			}
		}
		if(list.size() > count){
			Collections.shuffle(list);
			list = list.subList(0, count);
		}
		return list;
	}
	
	/**
	 * 根据{@list}的值排序并生成路径为{@filePath}的索引文件
	 * @param list
	 * @param filePath
	 */
	public static void generatorIndexFileByList(List<Integer> list, String filePath){
		int size = list != null ? list.size() : 0;
		if(size > 0){
			BitSet bitSet = new BitSet(size);
			for(int i=0; i<size; i++){
				bitSet.set(list.get(i));
			}
			int length = bitSet.length();
			for(int j=0; j<length; j++){
				if(bitSet.get(j)){
					writeLineToFile(filePath, j+System.getProperty("line.separator"), true);
				}
			}
		}
	}
}
