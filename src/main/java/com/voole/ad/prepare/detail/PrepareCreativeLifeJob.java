package com.voole.ad.prepare.detail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.prepare.AbstractPrepareJob;

/**
 * 预处理1:
 * 查询有效排期内节目并写到本地文件
 * @author shaoyl
 */
public class PrepareCreativeLifeJob extends AbstractPrepareJob{
	private String filePath; //文件生成路径
	private String fileName; //文件名称
	private String preSql;//查询有效排期节目sql语句
    private String toTimeFormat = "yyyy-MM-dd HH:mm:ss";//时间格式化
	
	@Autowired
	public JdbcTemplate adSupersspJt;
	

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String time) {
		logger.info("--------------进入排期内有效节目预处理-------------------------");
		long start  = System.currentTimeMillis();
		DateTimeFormatter localfromat = DateTimeFormat.forPattern("yyyyMMdd");
		DateTime localdate = DateTime.parse(time, localfromat);
		String toTime = localdate.toString(toTimeFormat);
		String newSql = preSql.replaceAll("@yestoday", toTime);
		logger.info("preSql 替换后为："+newSql);
		List<Map<String,Object>> creativeInfoList =  adSupersspJt.queryForList(newSql);
		//循环写入到文件
		File fileDir = new File(filePath);
		if (!fileDir.exists()) {  
			fileDir.mkdirs();  
        }  
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			String path = filePath+File.separator+fileName;
			File file =  new File(path);
			logger.info("whole file path is : " +  path);
			fw = new FileWriter(file);  
			bw = new BufferedWriter (fw);
			
			DateTime dt = new DateTime();
			String currDt = dt.toString("yyyyMMddHHmmss");
			
			for(Map<String,Object> creativeInfoMap:creativeInfoList){
				String creativeId = creativeInfoMap.get("creativeid").toString();
				String startDay = creativeInfoMap.get("startday").toString();
				String endDay = creativeInfoMap.get("endday").toString();
				bw.write(creativeId+","+startDay+","+endDay+","+currDt);
				bw.newLine();
			}
			
			bw.flush();
			bw.close();
			fw.close();
		} catch (IOException e) {
			logger.error("写入到文件["+fileName+"]出错:",e);
		}  finally{
        	try {
        	
		        	if(bw!=null){
						bw.close();
		        		bw = null;
		        	}
					if(fw!=null){
						fw.close();
						fw = null;
					}
        	
        	} catch (IOException e) {
        		logger.error("关闭流出错！",e);
			}
        }
		long end  = System.currentTimeMillis();
		
		logger.info("---------------有效排期节目表文件["+fileName+"]生成完成,耗时["+(end-start)+"]毫秒!------------------------");
	}


	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getPreSql() {
		return preSql;
	}

	public void setPreSql(String preSql) {
		this.preSql = preSql;
	}

    public String getToTimeFormat() {
        return toTimeFormat;
    }

    public void setToTimeFormat(String toTimeFormat) {
        this.toTimeFormat = toTimeFormat;
    }



	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

}
