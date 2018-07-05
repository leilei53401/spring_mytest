package com.voole.ad.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 应用程序启动类
* @author shaoyl
* @date 2017-4-9 上午10:56:22 
* @version V1.0
 */
public class StartUp {
	private static Logger log = Logger.getLogger(StartUp.class);
	public static void main(String[] args) throws IOException {
		//初始化日志
		initLogger();
		log.info("程序开始启动...");
		long beginTime = System.currentTimeMillis();
		try {
			final ApplicationContext ac = new  ClassPathXmlApplicationContext("classpath:"+Constants.ADSTAT_SPRING_CONF_FILE);
			/*CreateHivePartitionService bean = ac.getBean(CreateHivePartitionService.class);
			bean.createHivePartition();*/
		} catch (Exception e) {
			e.printStackTrace();
			log.error("加载spring配置文件失败！");
			System.exit(0);
		}
		long startUpTime = System.currentTimeMillis() - beginTime;
		log.info("程序启动成功,耗时" + startUpTime + "ms.");

	}
	
	
	/**
	 * 初始化日志器
	 * 
	 * @throws Exception
	 */
	private static void initLogger() {
		Properties props = new Properties();
		InputStream fileIn = null;
		try{
			fileIn = StartUp.class.getClassLoader().getResourceAsStream("./conf/log4j.properties");
            if (fileIn == null) {
            	System.err.println("get file from file path!");
                fileIn = new FileInputStream(Constants.ADSTAT_LOG_CONF_FILE);
                System.err.println("Can't get file from class path!");
            }
			props.load(fileIn);
			log.info("Successful to load logproperties!");
		}catch(Exception e){
			log.error("读取日志文件时出现错误",e);
			System.err.println("读取日志文件时出现错误"+e);
		}finally{
			if (fileIn != null) {
				try {
					fileIn.close();
				} catch (IOException e) {
					log.error("读取日志配置文件关闭流时出现错误",e);
					System.err.println("读取日志配置文件关闭流时出现错误"+e);
				}
			}
		}
		PropertyConfigurator.configure(props);
//		PropertyConfigurator.configureAndWatch(Constants.ADSTAT_LOG_CONF_FILE); 
	}
}
