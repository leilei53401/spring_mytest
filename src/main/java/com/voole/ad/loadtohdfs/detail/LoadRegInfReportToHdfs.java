package com.voole.ad.loadtohdfs.detail;

import com.voole.ad.loadtohdfs.AbstractBaseLoadJob;
import com.voole.ad.loadtohdfs.HdfsOp;
import com.voole.ad.loadtohdfs.UploadFileThread;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
* @Description: 注册，投放报表明细数据导入
* @author shaoyl
* @date 2017-4-3 下午11:21:23 
* @version V1.0
 */
public class LoadRegInfReportToHdfs extends AbstractBaseLoadJob {

    //hdfs 相关
    protected String hadoopUser;

    protected String hdfsRootPath;


	@Override
	public void process(String time) {
        log.info("------ start load 路径为【"+srcPath+"】下指标组为【"+kpiType+"】日期为【"+time+"】的文件------");

        long startTime = System.currentTimeMillis();

        //初始化线程池
   /*     BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, activeAlive, TimeUnit.DAYS, queue);
*/
        File filePath = new File(srcPath);
        File[] files = filePath.listFiles(
                new FileFilter() {
                    public boolean accept(File file) {
                        return file.getName().endsWith(".out");
                    }
                });


        if(null!=files && files.length>0){
            //导入文件
            for(File file:files){

                String fileName = file.getName();

                if(file.isFile() && file.length() > 0){
                    String[] array = StringUtils.split(fileName,".");

                    if(array.length<3){
                        log.info("解析文件名称["+fileName+"]发生异常!");
                        continue;
                    }

//                    String[] dayAndOemid = StringUtils.split(array[1],"_");

                    String daytime = array[1];
//                    String oemid = dayAndOemid[1];

                    String fileLocalPath = file.getPath();

                    String hdfsPath = getWholHdfsPath(daytime);

                    log.info("开始创建HDFS目录:"+hdfsPath);
                    HdfsOp.createDir(hdfsPath);

                    String hdfsFilePath = hdfsPath + fileName;


                    log.info("开始导入文件: "+ fileName +"...");

                    boolean ifTrue = HdfsOp.uploadFile(true, true, fileLocalPath, hdfsFilePath,
                            fileName,hadoopUser,hdfsRootPath);

                    log.info("导入文件: [" + fileName + "]完成,Flag=" + ifTrue);

//                    executor.execute(new UploadFileThread(file,hdfsPath));

                }
            }
        }else{
            log.warn("未过滤到 .out 文件， 无文件导入！");
        }

       /* executor.shutdown();
        try {
            executor.awaitTermination(100,TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error(e);
        }*/
        Long endTime = System.currentTimeMillis();
        log.info("指标组【"+kpiType+"】导入任务成功上传 ["+files.length+"] 个日志文件到hdfs上耗时: "+(endTime-startTime)/1000+" s");

    }



	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

    //获取hdfs path 全路径
//    private String getWholHdfsPath(String daytime,String oemid){
//        String hdfsPath = dstPath.replaceAll("\\{time\\}",daytime).replaceAll("\\{oemid\\}",oemid);
//        return hdfsPath;
//    }


    @Override
    public String getHadoopUser() {
        return hadoopUser;
    }

    @Override
    public void setHadoopUser(String hadoopUser) {
        this.hadoopUser = hadoopUser;
    }

    @Override
    public String getHdfsRootPath() {
        return hdfsRootPath;
    }

    @Override
    public void setHdfsRootPath(String hdfsRootPath) {
        this.hdfsRootPath = hdfsRootPath;
    }
}
