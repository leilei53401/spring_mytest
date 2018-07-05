package com.voole.ad.send;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.voole.ad.utils.GlobalProperties;

@Service
public class DataBakService{ 
	private Logger logger = LoggerFactory.getLogger(DataBakService.class);
	//Map中放各个文件的缓冲List，key是：文件路径_文件名  value:含有即将写入该文件内容的List
	//--private Map<String,List<String>> bufferMap = new ConcurrentHashMap<String,List<String>>();
	//与上面的map对应，记录特定key的缓存最后修改时间
	//key是：文件路径_文件名, value：bufferMap中对应List最后修改时间
	//--private Map<String,Long> buffLastModTimeMap = new ConcurrentHashMap<String,Long>();
	//--private Long lastTime = new Date().getTime(); //
	//--private Long currentTime = new Date().getTime();
	//系统分隔符
	private String sysSeparator = System.getProperty("line.separator"); 
	private int bakCorePoolSize=GlobalProperties.getInteger("thread.bak.total.corePoolSize");
	private int bakMaxPoolSize=GlobalProperties.getInteger("thread.bak.total.maxPoolSize");
	private int bakThreadAliveTime=GlobalProperties.getInteger("thread.bak.total.keepAliveTime");
	private ExecutorService pool = new ThreadPoolExecutor( bakCorePoolSize,bakMaxPoolSize,bakThreadAliveTime, TimeUnit.MILLISECONDS, 
														   new LinkedBlockingQueue<Runnable>(),new ThreadFactoryBuilder().setNameFormat("DataBakPool").build());
	
	//private ExecutorService pool = new ThreadPoolExecutor(10, 20, 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(2000), new DiscardOldestPolicy());
	
	public DataBakService(){

	}
	
	public void saveBakData(String filePath1, String fileName1, String params1) {
		// TODO Auto-generated method stub
		final String filePath = filePath1;
		final String fileName = fileName1;
		final String params = params1;
		//线程pool保存
		pool.execute(new Runnable(){
			@Override
			public void run() {
				// TODO Auto-generated method stub
				getFileAndWrite(filePath,fileName,params);
			}
			
			private void getFileAndWrite(String filePath,String fileName,String params){
				File file = FileUtils.getFile(new File(filePath),fileName+".txt");
				try {
					FileUtils.writeStringToFile(file, params+sysSeparator,true);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error("write params to file error!",e);
				}
			}
		});
	}
	/*测试中备份方法 
	public void saveBakData1(String filePath1, String fileName1, String params1) {
		// TODO Auto-generated method stub
		//定时 30分钟检查一次 有没有文件由于没达到数目没写入--------------------------------------------
		currentTime = new Date().getTime();
		if(currentTime - lastTime >= 20000){ //2分钟检查一次
			logger.info("定时检查时间到");
			//修改lastTime
			lastTime = currentTime;
			//遍历Map看有没有文件超时没写,超时则写入文件，并将该缓存信息从map中移走
			Set<String> keys = buffLastModTimeMap.keySet();
			for(String key: keys){
				long cacheLastModTime = buffLastModTimeMap.get(key);
				if(cacheLastModTime>=10000){
					List<String> cacheList = bufferMap.get(key);
					logger.info("发现超时缓存,大小 "+ cacheList.size());
					String strs="";
					for(String str:cacheList){
						strs=strs+str;
					}
					cacheList.clear();
					buffLastModTimeMap.remove(key);
					bufferMap.remove(key);
					final String params=strs;
					final String filePath=key.split("_")[0];
					final String fileName=key.split("_")[1];
					//写入
					pool.execute(new Runnable(){
						@Override
						public void run() {
							// TODO Auto-generated method stub
							//logger.info("写文件。。。");
							getFileAndWrite(filePath,fileName,params);
						}
						private void getFileAndWrite(String filePath,String fileName,String params){
							File file = FileUtils.getFile(new File(filePath),fileName);
							try {
								FileUtils.writeStringToFile(file, params,true);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								logger.error("write params to file error!");
							}
						}
					});
				}
			}
		}
		//--------------------定时检查结束-----------------------------------------------------
		//先从map中取，看某一文件的缓冲List是不是在map中；在，取出用；不在new；
		String mapKey = filePath1+"_"+fileName1;
		List<String> cacheList = bufferMap.get(mapKey);
		long lastModTime = 0;
		if(cacheList==null){ //不在map中：新建该文件缓存List
			cacheList = new ArrayList<String>();
			//cacheList = new CopyOnWriteArrayList<String>();
		}
		//数据放入缓存List，并更新Map
		cacheList.add(filePath1+","+fileName1+","+params1+sysSeparator);
		bufferMap.put(mapKey, cacheList);
		//更新最后修改时间戳
		lastModTime = new Date().getTime();
		buffLastModTimeMap.put(mapKey,lastModTime);
		//1、缓存大于1000条写入 
		if(cacheList.size()>=200){
			logger.info("size >=1");
			String strs="";
			for(String str:cacheList){
				strs=strs+str;
			}
			//移除该缓存
			cacheList.clear();
			bufferMap.remove(mapKey);
			buffLastModTimeMap.remove(mapKey);
			final String params=strs;
			final String filePath=filePath1;
			final String fileName=fileName1;
			pool.execute(new Runnable(){
				@Override
				public void run() {
					// TODO Auto-generated method stub
					logger.info("write file");
					getFileAndWrite(filePath,fileName,params);
				}
				
				private void getFileAndWrite(String filePath,String fileName,String params){
					File file = FileUtils.getFile(new File(filePath),fileName);
					try {
						FileUtils.writeStringToFile(file, params,true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						logger.error("write params to file error!");
					}
				}
			});
		}
	}*/
}
