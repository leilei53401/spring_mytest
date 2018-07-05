package com.voole.ad.file.detail;


import antlr.Lookahead;
import com.voole.ad.cache.AdCacheInfoService;
import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.utils.AdFileTools;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 解析本地文件任务
 * @author shaoyl
 *
 */

public class LocalFileParseJob  extends AbastractBaseFileJob {
	protected static Logger logger = Logger.getLogger(LocalFileParseJob.class);

    public static DateTimeFormatter fmt = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");
    public static DateTimeFormatter englishFmt = fmt.withLocale(Locale.ENGLISH);
	

	
	protected   String localPath;//本地路径配置
    protected  String backupPath;//解析完文件备份路径
    protected  String outPath;//解析后文件路径

    protected int maxProcessfiles = 5;//每次任务最多处理文件个数

    protected int batchSize = 1000;


    @Autowired
    public AdCacheInfoService adCacheInfoService;

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }


    @Override
    public void process(String time){
        long start  = System.currentTimeMillis();
        logger.info("开始执行下载任务...");
        parseLocalFiles();
        long end = System.currentTimeMillis();
        logger.info("结束执行下载任务,耗时["+(end-start)+"]毫秒");
    }


	//下载任务入口
	public void parseLocalFiles(){
		
			File f = new File(localPath);
			// 修改为过滤出文件名
			String[] listFiles = f.list(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					if (name.endsWith(".gz")) {
						return true;
					}
					return false;
				}
			});
			
			
			//限制每次任务最多处理文件个数。
			int allSize = 0;
			if(null != listFiles){
				allSize = listFiles.length;
			}
			
			
			List<String> processList = new ArrayList<String>();
			int toProcessSize = (allSize>maxProcessfiles)?maxProcessfiles:allSize;
			for(int i=0; i < toProcessSize;i++){
				processList.add(listFiles[i]);
			}
			
			logger.info("将要处理的文件为:"+processList.toString());
			logger.info("localpath ["+localPath+"] 过滤到 .gz 文件共["+allSize+"]个,每次任务最多处理["+maxProcessfiles+"]个!");
			
		/*	List<String> toCutFiles = new ArrayList<String>();
			List<String> excepFiles = new ArrayList<String>();*/
			for (String gzFile : processList) {
			    //解压gz 文件
//                boolean unCompressGzFlag = true;
                logger.info("----------开始解压文件["+gzFile+"]......-----------");
                boolean unCompressGzFlag = unCompressArchiveGz(gzFile);
                logger.info("解压文件["+gzFile+"]完成,结果为:"+unCompressGzFlag);
                //解压tar 文件
                String tarFile  = gzFile.replaceAll(".gz","");
                boolean unCompressTarFlag = false;
                if(unCompressGzFlag){
                    logger.info("----------开始解压文件["+tarFile+"]......-----------");
                    unCompressTarFlag = unCompressTar(tarFile);
                }

                logger.info("解压文件["+tarFile+"]完成,结果为:"+unCompressTarFlag);

			    //解析解压有文件
                String parseFile  = tarFile.replaceAll(".tar","");
                boolean result = false;
                if(unCompressGzFlag && unCompressTarFlag){
                    logger.info("----------开始解析文件["+parseFile+"]......-----------");
                    result = parseFile(parseFile);
                }
                logger.info("处理["+parseFile+"]完成,result=["+result+"]");
			}
				
	}


    /**
     * 解压 gz
     * @param archive
     * @throws IOException
     * @date 2017年5月27日下午4:03:29
     */
    private  boolean unCompressArchiveGz(String archive)  {
        BufferedOutputStream bos = null;
        GzipCompressorInputStream gcis = null;
        try {
            File file = new File(localPath+archive);

            if(!file.exists()){
                logger.warn("["+archive+"]文件不存在！");
                return false;
            }

            BufferedInputStream bis =
                    new BufferedInputStream(new FileInputStream(file));

            String fileName = file.getName().substring(0, file.getName().lastIndexOf("."));

            String finalName = file.getParent() + File.separator + fileName;

            bos = new BufferedOutputStream(new FileOutputStream(finalName));

            gcis = new GzipCompressorInputStream(bis);

            byte[] buffer = new byte[1024];
            int read = -1;
            while ((read = gcis.read(buffer)) != -1) {
                bos.write(buffer, 0, read);
            }
            gcis.close();
            bos.close();

            //删除gz文件
            logger.info("删除gz文件："+file.delete());

//            unCompressTar(finalName);
        } catch (Exception e) {
            logger.error("解压gz文件["+archive+"]出现异常:",e);
            return false;
        } finally {
            try {
                if(null!=gcis){
                    gcis.close();
                }
                if(null!=bos){
                    bos.close();
                }
            } catch (IOException e) {
               logger.error("关闭流出错：",e);
            }
        }
        return  true;
    }

    /**
     * 解压tar
     * @param finalName
     * @author yutao
     * @throws IOException
     * @date 2017年5月27日下午4:34:41
     */
    private boolean unCompressTar(String finalName) {

        TarArchiveInputStream tais = null;
        BufferedOutputStream bos = null;

        try {
            File file = new File(localPath+finalName);

            if(!file.exists()){
                logger.warn("["+finalName+"]文件不存在！");
                return false;
            }

            String parentPath = file.getParent();
            tais =  new TarArchiveInputStream(new FileInputStream(file));

            TarArchiveEntry tarArchiveEntry = null;

            while((tarArchiveEntry = tais.getNextTarEntry()) != null){
                String name = tarArchiveEntry.getName();
                File tarFile = new File(parentPath, name);
                if(!tarFile.getParentFile().exists()){
                    tarFile.getParentFile().mkdirs();
                }

                bos =  new BufferedOutputStream(new FileOutputStream(tarFile));

                int read = -1;
                byte[] buffer = new byte[1024];
                while((read = tais.read(buffer)) != -1){
                    bos.write(buffer, 0, read);
                }
                bos.close();
            }
            tais.close();

            logger.info("删除tar文件："+file.delete());

        } catch (Exception e) {
            logger.error("解压tar文件["+finalName+"]出错:",e);
            return false;
        } finally {

            try {
                if(null!=bos){
                    bos.close();
                }
                if(null!=tais){
                    tais.close();
                }
            } catch (IOException e) {
                logger.error("关闭流出错:",e);
            }
        }

        return true;
    }

    /**
     * 解析文件
      * @param inputFile
     * @return
     */
	public boolean parseFile(String inputFile) {
	    logger.info("------开始处理文件["+inputFile+"]-------");
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
					//数据样例：
                    //15/Jan/2018:18:57:01,900109102,bc307dea2354,100.116.204.16,4.2,4.4
                    String[] dataArray =  StringUtils.split(line,",");

        /*            create table superssp_reg_log
                            (
                                    starttime bigint,
                                    mac string,
                                    ip string,
                                    provinceid string,
                                    cityid string,
                                    cv string,
                                    sv string
                            )
                    partitioned by (daytime bigint,oemid int)
                    */

                    //时间
                    if(dataArray.length>=6){

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
                                .append(dataArray[4]).append(",")//cv
                                .append(dataArray[5]).append(",")//sv
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
			logger.info("=== 处理["+inputFile+"]结束,处理数据["+num+"]条!===");
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
						logger.info("备份文件["+inputFile+"]到["+backFile+"]下完成，flag="+flag);
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
