package com.voole.ad.main;


/**
 * 常量定义类
* @author shaoyl
* @date 2017-4-6 下午8:25:48 
* @version V1.0
 */
public class Constants {
	//工程根目录
	public static final String PROJECT_HOME = ".";//myeclipse环境用一个点
	//public static final String PROJECT_HOME = "..";//for部署，发布实际用两个点
	//日志文件属性配置
	public static final String ADSTAT_LOG_CONF_FILE = PROJECT_HOME.concat("/conf/log4j.properties");
	
	
	/**********************spring bean 配置文件 begin***************************/
	//(**修改 applicationContext.xml 配置不同的任务，不在这里指定不同配置文件)
	public static final String ADSTAT_SPRING_CONF_FILE = PROJECT_HOME.concat("\\conf\\applicationContext.xml");
	//库存配置文件
//	public static final String ADSTAT_SPRING_CONF_FILE = PROJECT_HOME.concat("/conf/applicationContext-inv.xml");
	
	/**********************spring bean 配置文件 end***************************/
	
}
