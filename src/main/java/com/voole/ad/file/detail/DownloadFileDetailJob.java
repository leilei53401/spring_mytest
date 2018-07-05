package com.voole.ad.file.detail;

import com.voole.ad.file.AbastractBaseFileJob;
import com.voole.ad.file.ftp.FTPClientException;
import com.voole.ad.file.ftp.FTPClientTools;
import org.apache.log4j.Logger;
import java.util.*;

/**
 * 注册，投放报表文件下载服务类
 */
public class DownloadFileDetailJob extends AbastractBaseFileJob {
	private static Logger logger = Logger.getLogger(DownloadFileDetailJob.class);

    FTPClientTools ftpClientTools;

    String fileNameExp;


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
        doDownLoadFiles();
        long end = System.currentTimeMillis();
        logger.info("结束执行下载任务,耗时["+(end-start)+"]毫秒");
    }


    public void doDownLoadFiles(){

        try {

            String[] okFileNames = ftpClientTools.listNamesByExp("*.ok",false);
            List<String> toDownFiles = new ArrayList<String>();
            for(String okFileName:okFileNames){
                String toDownLoadFileName = okFileName.replaceAll(".ok","");
                toDownFiles.add(toDownLoadFileName);
            }

            //记录下载状态
            Map<String,Integer> downLoadStatusMap = new HashMap<String,Integer>();

            //下载正常的文件可删除ftp上文件
            List<String> toDelFiles = new ArrayList<String>();

            if(null!=toDownFiles && toDownFiles.size()>0){

                //执行下载
                try {
                    downLoadStatusMap = ftpClientTools.downFiles(toDownFiles,false);
                } catch (FTPClientException e) {
                    logger.error("下载文件出错:",e);
                }


                Iterator<String> itStatus = downLoadStatusMap.keySet().iterator();
                while(itStatus.hasNext()){
                    String name = itStatus.next();
                    Integer status = downLoadStatusMap.get(name);
                    if(FTPClientTools.DOWNLOAD_OK==status){
                        String okFileName = name+".ok";
                        toDelFiles.add(okFileName);
                    }
                }

            }else{
                logger.warn("未获取到要下载的文件！");
            }


            //删除
            if(null!=toDelFiles && toDelFiles.size()>0){
                try {
                    String[] delFiles = new String[toDelFiles.size()];
                    toDelFiles.toArray(delFiles);
                    ftpClientTools.delete(delFiles, true);
                } catch (FTPClientException e) {
                    logger.error("删除ftp 上 ok 文件出错:",e);
                }
            }

        } catch (FTPClientException e) {
            logger.error("下载ftp上文件出错:",e);
        }finally {
            try {
                ftpClientTools.disconnect();
            } catch (FTPClientException e) {
                logger.error("退出ftp登录出错",e);
            }
        }

    }

    public FTPClientTools getFtpClientTools() {
        return ftpClientTools;
    }

    public void setFtpClientTools(FTPClientTools ftpClientTools) {
        this.ftpClientTools = ftpClientTools;
    }

    public String getFileNameExp() {
        return fileNameExp;
    }

    public void setFileNameExp(String fileNameExp) {
        this.fileNameExp = fileNameExp;
    }
}
