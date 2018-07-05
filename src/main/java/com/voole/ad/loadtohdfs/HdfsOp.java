package com.voole.ad.loadtohdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
/**
 * 上传hdfs工具类
* @author shaoyl
* @date 2017-4-10 下午3:51:27 
* @version V1.0
 */
public class HdfsOp {
	
	public static final Logger log = Logger.getLogger(HdfsOp.class);
	

	public static Configuration conf = null;

	static {

		try {
			conf = new Configuration();
//			conf.addResource("../conf/core-site.xml");
//			conf.addResource("../conf/hdfs-site.xml");
			
		} catch (Exception e) {
			log.error("加载hdfs配置文件出错：",e);
		}

	}
	
	//创建hdfs目录，先判断是否存在
	public static void createDir(String dirName){
		FileSystem fs = null;
		try{
			fs = FileSystem.get(conf);
			Path path = new Path(dirName);
			if(fs.exists(path)){ 
				log.info("path = "+path+",is exist!");
				return;
			}
			boolean flag = fs.mkdirs(path);
			
			log.info("create hdfs path "+path +",flag="+flag);
			
		}
		catch(Exception e){
			log.error("create hdfs path "+dirName+" fail");
			log.error(e);
		}
		
	}

	
	//上传本地文件
    public static boolean uploadFile(String src,String dst,String fileName){
    	  boolean code = true;
          Configuration conf = new Configuration();
          FileSystem fs =  null;
          Path srcPath = new Path(src); //原路径
          Path dstPath = new Path(dst); //目标路径
		try {
			fs = FileSystem.get(conf);
			if(fs.exists(new Path(dstPath+"/"+fileName))){
				log.warn(" hdfs file  "+dstPath+"/"+fileName +" is exsits ");
				return true;//存在的文件需要把本地删除，直接返回true 
			}
			fs.copyFromLocalFile(false,srcPath, dstPath);
			log.info("文件"+fileName+"导入完成!");
		} catch (IOException e) {
			code = false;
			log.error("文件"+fileName+"导入异常:",e);
			try {
				fs.delete(new Path(dstPath+"/"+fileName));
			} catch (IllegalArgumentException e1) {
				
			} catch (IOException e1) {
			
			}
		} 
		return code;
    }
    
   /**
    * 上传本地文件，并且是否覆盖hdfs上文件
    * @param delSrc
    * @param overwrite
    * @param src
    * @param dst
    * @param fileName
    * @return
    */
    public static boolean uploadFile(boolean delSrc, boolean overwrite,String src,String dst,String fileName,String hadoopUser,String hdfsRootPath){
    	  boolean code = true;
//    	    System.setProperty("HADOOP_USER_NAME", "root");//设置hadoop用户为root
    	  System.setProperty("HADOOP_USER_NAME", hadoopUser);
    		Configuration conf = new Configuration();
//			conf.set("fs.defaultFS", "hdfs://60.29.252.10:8082");
    		conf.set("fs.defaultFS", hdfsRootPath);
//          FileSystem fs =  null;
          Path srcPath = new Path(src); //本地源路径
          Path dstPath = new Path(dst); //目标路径
      	try {
//          FileSystem fs = dstPath.getFileSystem(conf);
      		FileSystem fs = FileSystem.get(conf);
//			fs = FileSystem.get(conf);
			fs.copyFromLocalFile(delSrc,overwrite,srcPath, dstPath);
			log.info("文件"+fileName+"导入完成!");
		} catch (IOException e) {
			code = false;
			log.error("文件"+fileName+"导入异常:",e);
		} 
		return code;
    }
}
