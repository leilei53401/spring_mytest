
package com.voole.ad.utils;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.sf.json.JSONObject;

public class HttpUtil {

	private static Logger log = Logger.getLogger(HttpUtil.class);
	/**
	 * post 请求发送,拿到返回流
	 * @param json
	 * @param POST_URL
	 * @throws IOException
	 */
    public static String sendPost(String url,String param){
    	String result = "";
    	DataOutputStream out = null;
    	HttpURLConnection connection = null;
    	BufferedReader reader = null;
        try{
    	  URL postUrl = new URL(url);
    	  connection = (HttpURLConnection)postUrl.openConnection();
          connection.setDoOutput(true);
          connection.setDoInput(true);
          connection.setRequestMethod("POST");
          connection.setUseCaches(false);
          connection.setInstanceFollowRedirects(true);
          connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
          connection.connect();
          out = new DataOutputStream(connection.getOutputStream());
          out.writeBytes(param);
         
       // 定义BufferedReader输入流来读取URL的响应
          reader = new BufferedReader(
                  new InputStreamReader(connection.getInputStream()));
          String line;
          while ((line = reader.readLine()) != null) {
        	  result += line;
          }
          //System.out.println(JSONObject.fromObject(result));
          int resultCode=connection.getResponseCode();
          if(resultCode != 200){
        	  log.debug("传输数据响应异常，异常编码为：result=" + resultCode + "异常信息：" + connection.getResponseMessage() + ",传输内容为："+ param);
          }
          out.flush();
        }catch(Exception e){
        	log.error("exception "+e);
        }finally{
        	try {
        		if(out !=null){
        	        out.close();
        		}
        		if(reader !=null){
        			reader.close();
        		}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
        	}
        }
        return result;
    }
    
    /**
     * 向指定URL发送GET方法的请求
     * 
     * @param url
     *            发送请求的URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return URL 所代表远程资源的响应结果
     */
    public static String sendGet(String url,String params) {
    	
    	String result = "";
        BufferedReader in = null;
        HttpURLConnection connection = null;
        try {
            String urlNameString = url;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            connection = (HttpURLConnection) realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("Connection", "close");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            //connection.setConnectTimeout(1000);
            //connection.setReadTimeout(2000);
            // 建立实际的连接
            connection.connect();
            //result = connection.getResponseCode()+"";
            //connection.disconnect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 遍历所有的响应头字段
            /*
            for (String key : map.keySet()) {
                 System.out.println(key + "--->" + map.get(key));
            }*/
            
           // result = connection.getResponseCode()+"";
            //检测是否有链接失败
           // List<String> states = map.get(null);
           // String state = states.get(0);
           // if("HTTP/1.1 200 OK".equals(state)){
            //	result="200";
            //}else{
            //	result=state;
            //}
            // log.info(connection.getResponseMessage());
            result = connection.getResponseCode()+"";
            //log.info(result);
            // 定义 BufferedReader输入流来读取URL的响应\
            /*
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }*/
        } catch (Exception e) {
            log.error("发送GET请求出现异常！", e);
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
                if(connection!=null){
                	connection.disconnect();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    	
    }
    
    /**
     * 根据请求类型区别请求方式
     * @param url
     * @param param
     * @param resType
     */
    public static String httpRequest(String url,String param,String resType){
    	String result = "";
    	if(null!=resType){
    		if("0".equals(resType)){  //0 get
    			result = sendGet(url,param);
    		}else if("1".equals(resType)){ //1 post
    			result = sendPost(url, param);
    		}
    	}
    	return result;
    }
    
    public static void main(String[] args) throws UnsupportedEncodingException {
		//String url = "http://adinf.voole.com:8080/movCallback";
    	String url = "http://localhost:8080/GUIDEINF/movCallback";
    	//String par="[{'rid':'111111','name':'五月天','systemid':'1','cpid':'1','mediainfos':[{'source':'http://www.xxxxxxx','type':'1','length':'30','pkgname':'ooo','action':'lll','activity':'mmm','startparam':'','ext':'123456789'},{'source':'http://www.xxxxxxx','type':'1','length':'30','pkgname':'','action':'','activity':'','startparam':'','ext':'000000'}]},{'rid':'22222','name':'网页','systemid':'2','cpid':'1','mediainfos':[{'source':'http://www.xxxxxxx','type':'1','length':'30','pkgname':'','action':'','activity':'','startparam':'','ext':'22222222'},{'source':'http://www.xxxxxxx','type':'1','length':'30','pkgname':'','action':'','activity':'','startparam':'','ext':'333333'}]}]";
    	String par = "[{\"rid\":\"275\",\"name\":\"龙龙直播\",\"systemid\":\"2\",\"cpid\":\"1\",\"mediainfos\":[{\"source\":\"http://store.7po.com/api/interface?mod=download&id=274&channel=QIPO_youpengpule_API\",\"type\":\"3\",\"length\":\"\",\"pkgname\":\"xlcao.sohutv4\",\"action\":\"\",\"activity\":\"\",\"startparam\":\"\"}]}]"; 
    	
    	String result = sendPost(url, URLEncoder.encode(par,"utf-8"));
    	System.out.println(result);
    	
    	//String url = "http://adinf.voole.com:8080/movChangeStauts";
    	//String par = "rids=2222211";
    	//sendGet(url, par);
	}
}
