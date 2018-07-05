package com.voole.ad.file.detail;


import com.voole.ad.cache.AdCacheInfoService;
import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.utils.AdFileTools;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 投放日志本地解析类
 * @author shaoyl
 *
 */

public class InfReportFileParseJob extends LocalFileParseJob {
	protected static Logger logger = Logger.getLogger(InfReportFileParseJob.class);

   /**
     * 解析文件
      * @param inputFile
     * @return
     */
    @Override
	public boolean parseFile(String inputFile) {
	    logger.info("------开始处理接口日志文件["+inputFile+"]-------");
		boolean result = true;
		File input = new File(localPath + inputFile);

        if(!input.exists()){
            logger.warn("["+inputFile+"]文件不存在！");
            return false;
        }
        //out 文件名
        String partFileName = inputFile.replaceAll(".txt","");

//        List<String> writeStringList = new ArrayList<String>();

		BufferedReader br = null;
        long num  = 0L;
		try {
			br = new BufferedReader(new FileReader(input));
			String line = null;
	//		PlayLogBean playLogBean = null;
			while ((line = br.readLine()) != null) {
				try {
//					logger.debug("开始处理播放串["+line+"]");
					//投放日志数据样例：
                    //20180311000000,900109101,1899f5dff700,223.73.136.143,4.2
                    String[] dataArray =  StringUtils.split(line,",");

        /*            create table superssp_inf_log
                        (
                            starttime bigint,
                            oemid bigint,
                            mac string,
                            ip string,
                            provinceid string,
                            cityid string,
                            version string,
                            hourtime int
                        )
                        partitioned by (daytime bigint)
                        ROW FORMAT DELIMITED
                        FIELDS TERMINATED BY ','
                        LINES TERMINATED BY '\n' ;
                    */

                    //时间
                    if(dataArray.length>=5){

                        StringBuilder sb = new StringBuilder();
                        //处理时间
                    /*    String time = dataArray[0];
                        DateTime dt = DateTime.parse(time, englishFmt);
                        String starttime =  dt.toString("yyyyMMddHHmmss");
                        String daytime =  dt.toString("yyyyMMdd");
                        String hourtime =  dt.toString("HH");*/

                        //处理时间(时间在脚本中已转化为yyyyMMddHHmmss,这里不需再处理)
                        String starttime = dataArray[0];
                        String daytime =  StringUtils.substring(starttime,0,8);
                        String hourtime =  StringUtils.substring(starttime,8,10);


                        //处理区域
                        String ip = dataArray[3];
                        String[] areainfo =  adCacheInfoService.getAreaInfo(ip);
                        String provinceid = areainfo[0];
                        String cityid = areainfo[1];


                        sb.append(starttime).append(",")
                                .append(dataArray[1]).append(",")//oemid
                                .append(dataArray[2]).append(",")//mac
                                .append(ip).append(",")
                                .append(provinceid).append(",")
                                .append(cityid).append(",")
                                .append(dataArray[4]).append(",")//version
                                .append(hourtime);
//                                String oemid = dataArray[1];
//                              String daytime = StringUtils.substring(starttime,0,8);

                        String outLine = sb.toString();

                        String outFileName = partFileName + "."+daytime+".out";

                        num++;
                        if(num%100000==0){
                            logger.info("+++++++++已处理["+inputFile+"]文件数据["+num+"]条！");
                        }
                        //写文件
                        AdFileTools.writeLineToFile(outPath,outFileName,outLine+"\n",true);

                    }else{
                        logger.warn("监测串["+line+"]长度不够!");
                    }

//					adAgent.sendSupersspAd(line);
				} catch (Exception e) {
					logger.error("处理数据["+line+"]异常:",e);
					continue;
				}
			}
			logger.info("=== 处理接口日志文件["+inputFile+"]结束,处理数据["+num+"]条!===");
			br.close();
		}catch (Exception ex) {
			logger.error("prcessFile ["+inputFile+"] error:" , ex);
			result = false;
		} finally {
			try {
				if (br != null){
					br.close();
					br = null;
				 }
				if(result){
					if (input != null && input.exists()) {
	//					input.delete();
						//备份到 backup目录下
						String backFilePath = "";
						if(backupPath.endsWith("/")){
							backFilePath = backupPath+inputFile;
						}else{
							backFilePath = backupPath+File.separator+inputFile;
						}
						File backFile =  new File(backFilePath);
						boolean flag = input.renameTo(backFile);
						logger.info("投放日志备份文件["+inputFile+"]到["+backFile+"]下完成，flag="+flag);
					}
				}
			} catch (Exception e) {
				logger.error("流关闭异常:", e);
			}
		}


        logger.info("------结束处理文件["+inputFile+"]-------");
		
		return result;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public String getBackupPath() {
		return backupPath;
	}

	public void setBackupPath(String backupPath) {
		this.backupPath = backupPath;
	}

    public String getOutPath() {
        return outPath;
    }

    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }

    public int getMaxProcessfiles() {
		return maxProcessfiles;
	}

	public void setMaxProcessfiles(int maxProcessfiles) {
		this.maxProcessfiles = maxProcessfiles;
	}
	

	
}
