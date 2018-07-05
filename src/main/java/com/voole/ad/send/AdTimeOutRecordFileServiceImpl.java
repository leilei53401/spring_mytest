package com.voole.ad.send;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.voole.ad.utils.GlobalProperties;

/**
 * 
 * 发送超时的记录文件
 * @author shaoyl
 *
 */
@Service
public class AdTimeOutRecordFileServiceImpl {

	private Logger logger = LoggerFactory.getLogger(AdTimeOutRecordFileServiceImpl.class);
	//private Collection<String> adUrlBackList = new ArrayList<String>();
	private String sendFailBakPath = GlobalProperties.getProperties("data.send.fail.bak.path").trim();
	private String bakLogic = GlobalProperties.getProperties("data.backup.dir.logic").trim();
	private File bakFile;
	private String sysSeparator = System.getProperty("line.separator"); 
	
	/**
	 * 重发失败后的备份
	 */
	public void writeFile2(String tableName,String url,int statusCode){
		   if(url != null && StringUtils.isNotBlank(url)){
	        	DateTime dateTime = new DateTime();
	            final String failUrl = dateTime.toString("yyyyMMddHHmmss")+" , "+statusCode+" , " + url + sysSeparator;
	            String catalogName = sendFailBakPath+"2/"+tableName + "/";
	            String dayTime = dateTime.toString("yyyyMMdd");
	            File file = FileUtils.getFile(new File(catalogName), dayTime+".txt");
	            try {
	                FileUtils.writeStringToFile(file, failUrl, true);
	            } catch (IOException e1) {
	                    logger.error("写发送失败备份文件发生错误！"+e1);
	            }
	        }
	}
	/**
	 * 
	 * 第一次发送失败备份
	 */
	public void writeFile1(String tableName,String url,int statusCode){
        if(url != null && StringUtils.isNotBlank(url)){
        	DateTime dateTime = new DateTime();
            final String failUrl = dateTime.toString("yyyyMMddHHmmss")+" , "+statusCode+" , " + url + sysSeparator;
            String catalogName = sendFailBakPath+"1/"+tableName + "/";
            String dayTime = dateTime.toString("yyyyMMdd");
            File file = FileUtils.getFile(new File(catalogName), dayTime+".txt");
            try {
                FileUtils.writeStringToFile(file, failUrl, true);
            } catch (IOException e1) {
                    logger.error("写发送失败备份文件发生错误！"+e1);
            }
        }
    }
	/**
	 * 异常数据备份
	 * @param adinfo
	 */
	public void exceptionWriteFile(String tableName,String url,String msg) {
		if(url != null && StringUtils.isNotBlank(url)){
        	DateTime dateTime = new DateTime();
            final String failUrl = dateTime.toString("yyyyMMddHHmmss")+" , " + "   ||   " + msg + "   ||   " + url + sysSeparator;
            String catalogName = sendFailBakPath+"-Exception/"+tableName + "/";
            String dayTime = dateTime.toString("yyyyMMdd");
            File file = FileUtils.getFile(new File(catalogName), dayTime+".txt");
            try {
                FileUtils.writeStringToFile(file, failUrl, true);
            } catch (IOException e1) {
                    logger.error("写发送失败备份文件发生错误！"+e1);
            }
        }
	}
}
