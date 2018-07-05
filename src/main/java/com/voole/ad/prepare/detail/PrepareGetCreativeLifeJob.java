package com.voole.ad.prepare.detail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.voole.ad.prepare.AbstractPrepareJob;

/**
 * 预处理1:
 * 远程查询有效排期内节目并写到本地文件
 * @author shaoyl
 */
public class PrepareGetCreativeLifeJob extends AbstractPrepareJob{
	private String filePath; //文件生成路径
	private String fileName; //文件名称
	private String urlPrefix;//RESTful url接口
	private String delimiter = ",";
	
	
	@Autowired
	public JdbcTemplate adSupersspJt;
	

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String time) {
		logger.info("--------------进入排期内有效节目预处理,time=["+time+"]-------------------------");
		JSONArray array = null;
		try {
			HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
			CloseableHttpClient httpclient = httpClientBuilder.build();
			// HttpClient httpclient = new DefaultHttpClient();
			// String uri =
			// "http://localhost:8080/springMVC/user/getUserByName/cwh";
			String uri = urlPrefix + time;
			logger.debug("uri=" + uri);
			HttpPost httppost = new HttpPost(uri);
			 array = new JSONArray();

			HttpResponse response;
			response = httpclient.execute(httppost);
			int code = response.getStatusLine().getStatusCode();
			logger.debug("code="+code);
			if (code == 200) {
				String rev = EntityUtils.toString(response.getEntity());
				// obj= JSONObject.fromObject(rev);
				array = JSONArray.fromObject(rev);
				// User user = (User)JSONObject.toBean(obj,User.class);
				logger.debug("返回数据===" + array.toString());
			}
		} catch (ClientProtocolException e) {
			logger.error("解析出错:",e);
			return;
		} catch (IOException e) {
			logger.error("解析出错:",e);
			return;
		} catch (Exception e) {
			logger.error("解析出错:",e);
			return;
		}  
		
		long start  = System.currentTimeMillis();
/*		preSql = preSql.replaceAll("@yestoday", time);
		logger.info("preSql 替换后为："+preSql);
		List<Map<String,Object>> creativeInfoList =  adSupersspJt.queryForList(preSql);*/
		
		if(null==array || array.size()==0){
			logger.error("未获取到time=["+time+"]有效排期创意数据！");
			return;
		}else{
			logger.info("获取到time=["+time+"]有效排期创意数据【"+array.size()+"】条！");
		}
		
		//##############将数据循环写入到文件###################
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
			
			for(int i=0;i<array.size();i++){
				JSONObject obj =  array.getJSONObject(i);
				String creativeId = obj.getString("creativeid");
				String startDay = obj.getString("startday");
				String endDay = obj.getString("endday");
				bw.write(creativeId+delimiter+startDay+delimiter+endDay+delimiter+currDt);
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

	public String getUrlPrefix() {
		return urlPrefix;
	}

	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}
	
	

}
