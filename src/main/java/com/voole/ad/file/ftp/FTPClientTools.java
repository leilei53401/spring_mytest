package com.voole.ad.file.ftp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
  
/** 
 * FTP客户端工具
 *  
 * @author shaoyl 
 */  
public class FTPClientTools {  
	public static final int DOWNLOAD_OK = 1;
	public static final int DOWNLOAD_ERR = 0;
	public static final int UPLOAD_OK = 1;
	public static final int UPLOAD_ERR = 0;
    //---------------------------------------------------------------------  
    // Instance data  
    //---------------------------------------------------------------------  
    /** logger */  
    protected final Logger log  = Logger.getLogger(getClass());  
    private ThreadLocal<FTPClient> ftpClientThreadLocal = new ThreadLocal<FTPClient>();  
  
    private String                 host;  
    private int                    port;  
    private String                 username;  
    private String                 password;  
  
    private boolean                binaryTransfer       = true;  
    private boolean                passiveMode          = true;  
    private String                 encoding             = "UTF-8";  
    private int                    clientTimeout        = 1000 * 30;  
    //新增
    private String                 remotePath;  
    private String                 localPath; 
    private String                 remoteBackupPath; 
    private int bufSize = 1024*200;
    
    public String getHost() {  
        return host;  
    }  
  
    public void setHost(String host) {  
        this.host = host;  
    }  
  
    public int getPort() {  
        return port;  
    }  
  
    public void setPort(int port) {  
        this.port = port;  
    }  
  
    public String getUsername() {  
        return username;  
    }  
  
    public void setUsername(String username) {  
        this.username = username;  
    }  
  
    public String getPassword() {  
        return password;  
    }  
  
    public void setPassword(String password) {  
        this.password = password;  
    }  
  
    public boolean isBinaryTransfer() {  
        return binaryTransfer;  
    }  
  
    public void setBinaryTransfer(boolean binaryTransfer) {  
        this.binaryTransfer = binaryTransfer;  
    }  
  
    public boolean isPassiveMode() {  
        return passiveMode;  
    }  
  
    public void setPassiveMode(boolean passiveMode) {  
        this.passiveMode = passiveMode;  
    }  
  
    public String getEncoding() {  
        return encoding;  
    }  
  
    public void setEncoding(String encoding) {  
        this.encoding = encoding;  
    }  
  
    public int getClientTimeout() {  
        return clientTimeout;  
    }  
  
    public void setClientTimeout(int clientTimeout) {  
        this.clientTimeout = clientTimeout;  
    }  
    
	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}
	
	public String getRemoteBackupPath() {
		return remoteBackupPath;
	}

	public void setRemoteBackupPath(String remoteBackupPath) {
		this.remoteBackupPath = remoteBackupPath;
	}
	
	
	public int getBufSize() {
		return bufSize;
	}

	public void setBufSize(int bufSize) {
		this.bufSize = bufSize;
	}

	//---------------------------------------------------------------------  
    // private method  
    //---------------------------------------------------------------------  
    /** 
     * 返回一个FTPClient实例 
     *  
     * @throws FTPClientException 
     */  
    private FTPClient getFTPClient() throws FTPClientException {  
        if (ftpClientThreadLocal.get() != null && ftpClientThreadLocal.get().isConnected()) {  
            return ftpClientThreadLocal.get();  
        } else {  
            FTPClient ftpClient = new FTPClient(); //构造一个FtpClient实例  
            ftpClient.setControlEncoding(encoding); //设置字符集  
      
            connect(ftpClient); //连接到ftp服务器  
      
            //设置为passive模式  
            if (passiveMode) {  
                ftpClient.enterLocalPassiveMode();  
            }  
            
            ftpClient.setBufferSize(bufSize);
            
            setFileType(ftpClient); //设置文件传输类型  
      
            try {  
                ftpClient.setSoTimeout(clientTimeout);  
            } catch (SocketException e) {  
                throw new FTPClientException("Set timeout error.", e);  
            }  
            ftpClientThreadLocal.set(ftpClient);  
            return ftpClient;  
        }  
    }  
  
    /** 
     * 设置文件传输类型 
     *  
     * @throws FTPClientException 
     * @throws IOException 
     */  
    private void setFileType(FTPClient ftpClient) throws FTPClientException {  
        try {  
            if (binaryTransfer) {  
                ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);  
            } else {  
                ftpClient.setFileType(FTPClient.ASCII_FILE_TYPE);  
            }  
        } catch (IOException e) {  
            throw new FTPClientException("Could not to set file type.", e);  
        }  
    }  
  
    /** 
     * 连接到ftp服务器 
     *  
     * @param ftpClient 
     * @return 连接成功返回true，否则返回false 
     * @throws FTPClientException 
     */  
    private boolean connect(FTPClient ftpClient) throws FTPClientException {  
        try {  
            ftpClient.connect(host, port);  
  
            // 连接后检测返回码来校验连接是否成功  
            int reply = ftpClient.getReplyCode();  
  
            if (FTPReply.isPositiveCompletion(reply)) {  
                //登陆到ftp服务器  
                if (ftpClient.login(username, password)) {  
                    setFileType(ftpClient);  
                    return true;  
                }  
            } else {  
                ftpClient.disconnect();  
                throw new FTPClientException("FTP server refused connection.");  
            }  
        } catch (IOException e) {  
            if (ftpClient.isConnected()) {  
                try {  
                    ftpClient.disconnect(); //断开连接  
                } catch (IOException e1) {  
                    throw new FTPClientException("Could not disconnect from server.", e1);  
                }  
  
            }  
            throw new FTPClientException("Could not connect to server.", e);  
        }  
        return false;  
    }  
  
  
    //---------------------------------------------------------------------  
    // public method  
    //---------------------------------------------------------------------  
    /** 
     * 断开ftp连接 
     *  
     * @throws FTPClientException 
     */  
    public void disconnect() throws FTPClientException {  
        try {  
            FTPClient ftpClient = getFTPClient();  
            ftpClient.logout();  
            if (ftpClient.isConnected()) {  
                ftpClient.disconnect();  
                ftpClient = null;  
            }  
        } catch (IOException e) {  
            throw new FTPClientException("Could not disconnect from server.", e);  
        }  
    }  
      
    public boolean mkdir(String pathname) throws FTPClientException {  
        return mkdir(pathname, null);  
    }  
      
    /** 
     * 在ftp服务器端创建目录（不支持一次创建多级目录） 
     *  
     * 该方法执行完后将自动关闭当前连接 
     *  
     * @param pathname 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean mkdir(String pathname, String workingDirectory) throws FTPClientException {  
        return mkdir(pathname, workingDirectory, true);  
    }  
      
    /** 
     * 在ftp服务器端创建目录（不支持一次创建多级目录） 
     *  
     * @param pathname 
     * @param autoClose 是否自动关闭当前连接 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean mkdir(String pathname, String workingDirectory, boolean autoClose) throws FTPClientException {  
        try {  
            getFTPClient().changeWorkingDirectory(workingDirectory);  
            return getFTPClient().makeDirectory(pathname);  
        } catch (IOException e) {  
            throw new FTPClientException("Could not mkdir.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //断开连接  
            }  
        }  
    }  
  
    /** 
     * 上传一个本地文件到远程指定文件 
     *  
     * @param remoteAbsoluteFile 远程文件名(包括完整路径) 
     * @param localAbsoluteFile 本地文件名(包括完整路径) 
     * @return 成功时，返回true，失败返回false 
     * @throws FTPClientException 
     */  
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile) throws FTPClientException {  
        return put(remoteAbsoluteFile, localAbsoluteFile, true);  
    }  
  
    /** 
     * 上传一个本地文件到远程指定文件 
     *  
     * @param remoteAbsoluteFile 远程文件名(包括完整路径) 
     * @param localAbsoluteFile 本地文件名(包括完整路径) 
     * @param autoClose 是否自动关闭当前连接 
     * @return 成功时，返回true，失败返回false 
     * @throws FTPClientException 
     */  
    public boolean put(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws FTPClientException {  
        InputStream input = null;  
        try {  
            // 处理传输  
            input = new FileInputStream(localAbsoluteFile);  
            getFTPClient().storeFile(remoteAbsoluteFile, input);  
            log.debug("put " + localAbsoluteFile);  
            return true;  
        } catch (FileNotFoundException e) {  
            throw new FTPClientException("local file not found.", e);  
        } catch (IOException e) {  
            throw new FTPClientException("Could not put file to server.", e);  
        } finally {  
            try {  
                if (input != null) {  
                    input.close();  
                }  
            } catch (Exception e) {  
                throw new FTPClientException("Couldn't close FileInputStream.", e);  
            }  
            if (autoClose) {  
                disconnect(); //断开连接  
            }  
        }  
    }  
  
    /** 
     * 下载一个远程文件到本地的指定文件 
     *  
     * @param remoteAbsoluteFile 远程文件名(包括完整路径) 
     * @param localAbsoluteFile 本地文件名(包括完整路径) 
     * @return 成功时，返回true，失败返回false 
     * @throws FTPClientException 
     */  
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile) throws FTPClientException {  
        return get(remoteAbsoluteFile, localAbsoluteFile, true);  
    }  
  
    /** 
     * 下载一个远程文件到本地的指定文件 
     *  
     * @param remoteAbsoluteFile 远程文件名(包括完整路径) 
     * @param localAbsoluteFile 本地文件名(包括完整路径) 
     * @param autoClose 是否自动关闭当前连接 
     *  
     * @return 成功时，返回true，失败返回false 
     * @throws FTPClientException 
     */  
    public boolean get(String remoteAbsoluteFile, String localAbsoluteFile, boolean autoClose) throws FTPClientException {  
        OutputStream output = null;  
        try {  
            output = new FileOutputStream(localAbsoluteFile);  
            return get(remoteAbsoluteFile, output, autoClose);  
        } catch (FileNotFoundException e) {  
            throw new FTPClientException("local file not found.", e);  
        } finally {  
            try {  
                if (output != null) {  
                    output.close();  
                }  
            } catch (IOException e) {  
                throw new FTPClientException("Couldn't close FileOutputStream.", e);  
            }  
        }  
    }  
  
    /** 
     * 下载一个远程文件到指定的流 处理完后记得关闭流 
     *  
     * @param remoteAbsoluteFile 
     * @param output 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean get(String remoteAbsoluteFile, OutputStream output) throws FTPClientException {  
        return get(remoteAbsoluteFile, output, true);  
    }  
  
    /** 
     * 下载一个远程文件到指定的流 处理完后记得关闭流 
     *  
     * @param remoteAbsoluteFile 
     * @param output 
     * @param delFile 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean get(String remoteAbsoluteFile, OutputStream output, boolean autoClose) throws FTPClientException {  
        try {  
            FTPClient ftpClient = getFTPClient();  
            // 处理传输  
            return ftpClient.retrieveFile(remoteAbsoluteFile, output);  
        } catch (IOException e) {  
            throw new FTPClientException("Couldn't get file from server.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
    }  
    
    
    /** 
     * 下载多个文件
     *  
     * @param remoteAbsoluteFile 
     * @param output 
     * @param delFile 
     * @return Map<String,String>  key: filename; value: status ,
     * @throws FTPClientException 
     */  
    public Map<String,Integer> downFiles(List<String> remoteFiles,boolean autoClose) throws FTPClientException {  
    	int ok=0;
    	int err=0;
    	Map<String,Integer> statusMap = new HashMap<String,Integer>();
        try {  
            FTPClient ftpClient = getFTPClient(); 
            ftpClient.changeWorkingDirectory(remotePath);
            log.info("当前工作路径为"+ftpClient.printWorkingDirectory());
            long start_all = System.currentTimeMillis();
        	FileOutputStream fos =null;
            // 处理传输  
//            return ftpClient.retrieveFile(remoteAbsoluteFile, output);  
            for(String ftpFileName:remoteFiles){
            	long start = System.currentTimeMillis();
//            	String tmpName = ftpFileName.replaceAll(".txt",".tmp");
                String tmpName = ftpFileName + ".tmp";
				try {
					File realFile = new File(localPath + ftpFileName);
					File tmpFile = new File(localPath + File.separator + tmpName);
					fos = new FileOutputStream(tmpFile);
					if (ftpClient.retrieveFile(ftpFileName, fos)) {
						log.info("下载" + ftpFileName + "到" + localPath + "路径下成功!");
						fos.close();
						//下载成功后修改tmp结尾的文件名为原gz文件。
						if(!tmpFile.renameTo(realFile)){
							log.warn("临时文件【"+tmpFile+"】重命名出错！");
							err++;
							statusMap.put(ftpFileName, this.DOWNLOAD_ERR);
						}else{
							ok++;
							statusMap.put(ftpFileName, this.DOWNLOAD_OK);
						}
					} else {
						log.info("下载[" + ftpFileName + "]到[" + localPath + "]路径下失败!");
						int replyCode = ftpClient.getReplyCode();
						String replyStr = ftpClient.getReplyString();
						log.error("replyCode=" + replyCode + ";replyStr=" + replyStr);
						err++;
						statusMap.put(ftpFileName, this.DOWNLOAD_ERR);
						
						if(tmpFile.exists()){
							tmpFile.delete();	
						}
					}
					fos.close();
				} catch (Exception e) {
					log.error("下载[" + ftpFileName + "]到[" + localPath + "]路径下出错:",e);
					err++;
					statusMap.put(ftpFileName, this.DOWNLOAD_ERR);
					continue;
				}finally{
					if(fos!=null){
						fos.close();
						fos=null;
					}
				}
				
				long end = System.currentTimeMillis();
				log.info("下载" + ftpFileName + "完成，耗时["+(end-start)+"]毫秒!");
            }
            long end_all = System.currentTimeMillis();
            log.info("下载完成,成功【"+ok+"】条，失败【"+err+"】条!，耗时["+(end_all-start_all)+"]毫秒");
        } catch (IOException e) {  
            throw new FTPClientException("Couldn't get file from server.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
        return statusMap;
    }  
    
    
    /** 
     * 上传多个文件到ftp
     *  
     * @param remoteAbsoluteFile 
     * @param output 
     * @param delFile 
     * @return Map<String,String>  key: filename; value: status ,
     * @throws FTPClientException 
     */  
    public Map<String,Integer> uploadFiles(List<String> toUploadFiles,boolean autoClose) throws FTPClientException {  
    	int ok=0;
    	int err=0;
		Map<String, Integer> statusMap = new HashMap<String, Integer>();
		
		FTPClient ftpClient = getFTPClient();
		try {
			boolean mkdir = ftpClient.makeDirectory(remotePath);
			log.info("创建ftp目录[" + remotePath + "]：" + mkdir);
			boolean change = ftpClient.changeWorkingDirectory(remotePath);
			log.info("切换到工作目录[" + remotePath + "]：" + change);
			log.info("当前工作路径为" + ftpClient.printWorkingDirectory());
		} catch (IOException e) {
			throw new FTPClientException("Could not mkdir.", e);
		}
    	  
        try {  
          
            long start_all = System.currentTimeMillis();
            // 处理传输  
/*            input = new FileInputStream(localAbsoluteFile);  
            getFTPClient().storeFile(remoteAbsoluteFile, input);  
            log.debug("put " + localAbsoluteFile); 
             */
            FileInputStream input =null;
            // 处理传输  
//            return ftpClient.retrieveFile(remoteAbsoluteFile, output);  
            for(String localFileName:toUploadFiles){
            	long start = System.currentTimeMillis();
            	String tmpName = localFileName+".tmp";  
				try {
					File localFile = new File(localPath +File.separator+ localFileName);
//					File tmpFile = new File(localPath + tmpName);
					 input = new FileInputStream(localFile);  
						log.info("正在上传文件[" + localFileName + "]到[" + remotePath + "]路径，请稍后......");
					if (ftpClient.storeFile(tmpName, input)) {
						log.info("上传[" + localFileName + "]到[" + remotePath + "]路径下成功!");
						input.close();
						//上传成功后修改tmp结尾的文件名为原gz文件。
						try {
							boolean flag = ftpClient.rename(tmpName, localFileName);
							if(flag){
								log.info("tmp文件【"+tmpName+"】重命名为["+localFileName+"]完成！");
								statusMap.put(localFileName, this.UPLOAD_OK);
								//删除本地gz文件
								if(localFile.exists()){
									boolean localDelFlag = localFile.delete();
									log.info("本地删除["+localFileName+"]状态:"+localDelFlag);
								}
								ok++;
							}else{
								log.warn("tmp文件【"+tmpName+"】重命名为["+localFileName+"]失败！");
								statusMap.put(localFileName, this.UPLOAD_ERR);
								err++;
							}
						} catch (Exception e) {
							log.error("rename file ["+tmpName+"] error.", e);  
							statusMap.put(localFileName, this.UPLOAD_ERR);
							err++;
						}
						
					} else {
						log.info("上传[" + localFileName + "]到[" + remotePath + "]路径下失败!");
						int replyCode = ftpClient.getReplyCode();
						String replyStr = ftpClient.getReplyString();
						log.error("replyCode=" + replyCode + ";replyStr=" + replyStr);
						err++;
						statusMap.put(localFileName, this.UPLOAD_ERR);
						
						/*if(tmpFile.exists()){
							tmpFile.delete();	
						}*/
					}
					input.close();
				} catch (Exception e) {
					log.error("上传[" + localFileName + "]到[" + localPath + "]路径下出错:",e);
					err++;
					statusMap.put(localFileName, this.UPLOAD_ERR);
					continue;
				}finally{
					if(input!=null){
						input.close();
						input=null;
					}
				}
				
				long end = System.currentTimeMillis();
				log.info("下载" + localFileName + "完成，耗时["+(end-start)+"]毫秒!");
            }
            long end_all = System.currentTimeMillis();
            log.info("上传完成,成功【"+ok+"】条，失败【"+err+"】条!，耗时["+(end_all-start_all)+"]毫秒");
        } catch (IOException e) {  
            throw new FTPClientException("Couldn't get file from server.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
        return statusMap;
    }  
    
    /**
     * 备份ftp上文件
     * @param remoteFiles
     * @param autoClose
     * @return
     */
	public Map<String,Integer> backupFiles(List<String> remoteFiles, boolean autoClose) {  
		
		FTPClient ftpClient = null;
		try {
			ftpClient = getFTPClient();
		} catch (Exception e) {
			log.error("Couldn't get ftpClient.", e);  
		}
		
		Map<String,Integer> statusMap = new HashMap<String,Integer>();
        	
      try {
            ftpClient.changeWorkingDirectory(remotePath);
            log.info("当前工作路径为"+ftpClient.printWorkingDirectory());
            String bacupPath = "../"+remoteBackupPath+"/";
            
            for(String ftpFileName:remoteFiles){
            	try {
					boolean flag = ftpClient.rename(ftpFileName, bacupPath+ftpFileName);
					if(flag){
						log.info("ftp文件【"+ftpFileName+"】备份到 ftp目录["+bacupPath+"]下完成！");
						statusMap.put(ftpFileName, this.DOWNLOAD_OK);
					}else{
						log.warn("ftp文件【"+ftpFileName+"】备份到 ftp目录["+bacupPath+"]下失败！");
						statusMap.put(ftpFileName, this.DOWNLOAD_ERR);
					}
				} catch (Exception e) {
					log.error("backup file ["+ftpFileName+"] error.", e);  
					statusMap.put(ftpFileName, this.DOWNLOAD_ERR);
				}
            }
           
        } catch (Exception e) {  
        	log.error("backup file error.", e);  
        } finally {  
            if (autoClose) {  
                try {
					disconnect();
				} catch (FTPClientException e) {
					log.error("断开 ftp ["+host+"]连接出错:", e);  
				} //关闭链接  
            }  
        }  
      
      return statusMap;
    }  
  
    /** 
     * 从ftp服务器上删除一个文件 
     * 该方法将自动关闭当前连接 
     *  
     * @param delFile 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean delete(String delFile) throws FTPClientException {  
        return delete(delFile, true);  
    }  
      
    /** 
     * 从ftp服务器上删除一个文件 
     *  
     * @param delFile 
     * @param autoClose 是否自动关闭当前连接 
     *  
     * @return 
     * @throws FTPClientException 
     */  
    public boolean delete(String delFile, boolean autoClose) throws FTPClientException {  
        try {  
            getFTPClient().deleteFile(delFile);  
            return true;  
        } catch (IOException e) {  
            throw new FTPClientException("Couldn't delete file from server.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
    }  
      
    /** 
     * 批量删除 
     * 该方法将自动关闭当前连接 
     *  
     * @param delFiles 
     * @return 
     * @throws FTPClientException 
     */  
    public boolean delete(String[] delFiles) throws FTPClientException {  
        return delete(delFiles, true);  
    }  
  
    /** 
     * 批量删除 
     *  
     * @param delFiles 
     * @param autoClose 是否自动关闭当前连接 
     *  
     * @return 
     * @throws FTPClientException 
     */  
    public boolean delete(String[] delFiles, boolean autoClose) throws FTPClientException {  
        try {  
            FTPClient ftpClient = getFTPClient();  
            for (String s : delFiles) {  
                ftpClient.deleteFile(s);  
            }  
            return true;  
        } catch (IOException e) {  
            throw new FTPClientException("Couldn't delete file from server.", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
    }  
  
    /** 
     * 列出远程默认目录下所有的文件 
     *  
     * @return 远程默认目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组 
     * @throws FTPClientException 
     */  
    public String[] listNames() throws FTPClientException {  
        return listNames(null, true);  
    }  
      
    public String[] listNames(boolean autoClose) throws FTPClientException {  
        return listNames(null, autoClose);  
    }  
  
    /** 
     * 列出远程目录下所有的文件 
     *  
     * @param remotePath 远程目录名 
     * @param autoClose 是否自动关闭当前连接 
     *  
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组 
     * @throws FTPClientException 
     */  
    public String[] listNames(String remotePath, boolean autoClose) throws FTPClientException {  
        try {  
            String[] listNames = getFTPClient().listNames(remotePath);  
            return listNames;  
        } catch (IOException e) {  
            throw new FTPClientException("列出远程目录下所有的文件时出现异常", e);  
        } finally {  
            if (autoClose) {  
                disconnect(); //关闭链接  
            }  
        }  
    }

    /**
     * 列出远程目录下所有的文件
     *
     * @param expName 名称表达式
     * @param autoClose 是否自动关闭当前连接
     *
     * @return 远程目录下所有文件名的列表，目录不存在或者目录下没有文件时返回0长度的数组
     * @throws FTPClientException
     */
    public String[] listNamesByExp(String expName, boolean autoClose) throws FTPClientException {
        try {
//            String param = remotePath  + expName;
            FTPClient ftpClient = getFTPClient();
            log.info("work dir is : "+ftpClient.printWorkingDirectory());
            ftpClient.changeWorkingDirectory(remotePath);
            String[] listNames = ftpClient.listNames(expName);
            return listNames;
        } catch (IOException e) {
            throw new FTPClientException("列出远程目录下所有的文件时出现异常", e);
        } finally {
            if (autoClose) {
                disconnect(); //关闭链接
            }
        }
    }
  
    public static void main(String[] args) throws FTPClientException, InterruptedException {  
        FTPClientTools ftp = new FTPClientTools();  
        ftp.setHost("localhost");  
        ftp.setPort(2121);  
        ftp.setUsername("admin");  
        ftp.setPassword("admin");  
        ftp.setBinaryTransfer(false);  
        ftp.setPassiveMode(false);  
        ftp.setEncoding("utf-8");  
  
        //boolean ret = ftp.put("/group/tbdev/query/user-upload/12345678910.txt", "D:/099_temp/query/12345.txt");  
        //System.out.println(ret);  
        ftp.mkdir("asd", "user-upload");  
          
        //ftp.disconnect();  
        //ftp.mkdir("user-upload1");  
        //ftp.disconnect();  
          
        //String[] aa = {"/group/tbdev/query/user-upload/123.txt", "/group/tbdev/query/user-upload/SMTrace.txt"};  
        //ftp.delete(aa);  
    }  
}