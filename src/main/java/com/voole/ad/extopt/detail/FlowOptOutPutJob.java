package com.voole.ad.extopt.detail;

import com.alibaba.fastjson.JSON;
import com.voole.ad.cache.AdCacheInfoService;
import com.voole.ad.extopt.AbstractExtOptJob;
import com.voole.ad.utils.AdFileTools;
import com.voole.ad.utils.HttpClientUtil;
import net.sf.json.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.ClientProtocolException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 流量优化终端输出任务
 * @author shaoyl
 * @TODO:
 */
public class FlowOptOutPutJob extends AbstractExtOptJob{
	

    private String getTermNumsSql; //获取要计算的量
	private  String outPutTermNumsSql; //输出数据sql

    private String outPutPath; //输出路径

    private int stepSize; //输出多少天
	
	@Autowired
	public JdbcTemplate adSupersspJt;

    @Autowired
    public JdbcTemplate hiveJt;

    @Autowired
    public AdCacheInfoService adCacheInfoService;

    //要输出的media 和  oemid
//    private Map<String,HashSet<String>> mediaAndOemids;


	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	public void process(String time, Map<String,HashSet<String>> mediaAndOemids) {
		
		logger.info("------------开始进行终端数据导出任务----------------");
		long startTime = System.currentTimeMillis();

			long start  = System.currentTimeMillis();
			//查询数据

        if(mediaAndOemids!=null && mediaAndOemids.size()>0) {
            logger.info("进入流量优化输出任务，获取到媒体【"+mediaAndOemids.size()+"】条！");
            //循环媒体计算
            for (Iterator iterator = mediaAndOemids.keySet().iterator(); iterator.hasNext(); ) {

                String mediaid = (String) iterator.next();

                String getTermNumsSqlNew = getTermNumsSql.replaceAll("@daytime",time);
                getTermNumsSqlNew = getTermNumsSqlNew.replaceAll("@mediaid",mediaid);

                logger.info("查询需导出终端个数sql为:"+getTermNumsSqlNew);

                //查询需要导出个数
                List<Map<String, Object>> areaList = adSupersspJt.queryForList(getTermNumsSqlNew);


                DateTimeFormatter fromat = DateTimeFormat.forPattern("yyyyMMdd");
                DateTime dateTime = DateTime.parse(time, fromat);

                DateTime outDayTime = dateTime.plusDays(1);

                String outDayStr = outDayTime.toString("yyyyMMdd");

                if (null != areaList && areaList.size() > 0) {

                    logger.info("获取到mediaid=[" + mediaid + "] daytime=[" + time + "] outDay=["+outDayStr+"] 数据【" + areaList.size() + "】条!");

                    String outPath = outPutPath + "/" + mediaid +"/"+outDayStr;

                    logger.info("outPath=[" + outPath + "]!");

                    for(Map<String, Object> data:areaList){
                        String area_code = data.get("area_code").toString();
                        String termnums = data.get("termial_num").toString();

                        //获取省id
                        String areaPid = adCacheInfoService.getProvinceIdByCityId(area_code);

                        logger.info("开始导出mediaid=[" + mediaid + "] daytime=[" + time + "] outDay=["+outDayStr+"], areaPid=["+areaPid+"], area_code=["+area_code+"] 的终端【" + termnums + "】个!");

                        //根据指定终端个数，导出数据

                        String outSqlNew = outPutTermNumsSql.replaceAll("@mediaid",mediaid);

                        outSqlNew = outSqlNew.replaceAll("@provinceid",areaPid);

                        outSqlNew = outSqlNew.replaceAll("@cityid",area_code);

                        outSqlNew = outSqlNew + " limit " + termnums;

                        logger.info("查询需输出终端 hql 为:"+outSqlNew);

                        //查询输出数据
                        List<Map<String, Object>> outList = hiveJt.queryForList(outSqlNew);


                        if(null!=outList && outList.size()>0){


                        StringBuilder lineBuilder = new StringBuilder();

                        String fileName = outDayStr+"_"+area_code+".txt";

                            //如果文件存在，则删除更新
                        File newFile = new File(outPath+File.separator+fileName);

                        if(newFile.exists()){
                            boolean flag = newFile.delete();
                            logger.info("删除目录["+outPath+"]下文件["+fileName+"] flag = "+flag);
                        }


                        for(Map<String, Object> outData : outList){
                            String mac = (null==outData.get("flow_opt_base.mac"))?"":outData.get("flow_opt_base.mac").toString();
                            String oemid = (null==outData.get("flow_opt_base.oemid"))?"":outData.get("flow_opt_base.oemid").toString();
                            String ip = (null==outData.get("flow_opt_base.ip"))?"":outData.get("flow_opt_base.ip").toString();
                            String provinceid = (null==outData.get("flow_opt_base.provinceid"))?"0":outData.get("flow_opt_base.provinceid").toString();
                            String cityid = (null==outData.get("flow_opt_base.cityid"))?"0":outData.get("flow_opt_base.cityid").toString();

                            lineBuilder.append(mac).append(",")
                                    .append(oemid).append(",")
                                    .append(ip).append(",")
                                    .append(provinceid).append(",")
                                    .append(cityid).append(",")
                                    .append(3).append(",")
                                    .append(3).append("\n");

                            AdFileTools.writeLineToFile(outPath,fileName,lineBuilder.toString(),true);
                            lineBuilder.setLength(0);

                        }
                        //生成一个 .down 结尾的空文件表示完成
                        String overName = outDayStr+"_"+area_code+".txt.down";
                        AdFileTools.writeLineToFile(outPath,overName,"",true);
                        }else {
                            logger.info("mediaid=[" + mediaid + "] area_code=["+area_code+"] 未查询出终端数据!");
                        }
                    }

                    //此媒体目录下文件输出完成，生成一个空文件 down.txt
                    String folderDoneName = "down.txt";
                    AdFileTools.writeLineToFile(outPath,folderDoneName,"",true);

                    //######################### 开始复制生成之后几天的数据 #######################################


                    for (int i = 1; i <= stepSize; i++) {
                        DateTime createDateTime = outDayTime.plusDays(i);
                        String targetDayValue = createDateTime.toString("yyyyMMdd");

                        String toPath = outPath.replaceAll(outDayStr,targetDayValue);

                        logger.info("--------------------copy files to【"+ toPath +"】start !---------------------");

                        copyFileToPath(outPath, toPath, outDayStr, targetDayValue);

                        logger.info("--------------------copy files to 【"+ toPath +"】end ！---------------------");
                    }

                    //######################### 结束复制生成之后几天的数据 ##########################################

                }else {
                    logger.info("未获取到mediaid=[" + mediaid + "] daytime=[" + time + "], outDay=["+outDayStr+"] 输出终端数据!");
                }
            }
        }else{
            logger.info("没有流量输出任务需要执行！");
        }
		
		long endTime = System.currentTimeMillis();
		logger.info("-------------结束终端数据导出任务,耗时("+(endTime-startTime)/1000+")秒！------------------------");
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

    public String getGetTermNumsSql() {
        return getTermNumsSql;
    }

    public void setGetTermNumsSql(String getTermNumsSql) {
        this.getTermNumsSql = getTermNumsSql;
    }

    public String getOutPutTermNumsSql() {
        return outPutTermNumsSql;
    }

    public void setOutPutTermNumsSql(String outPutTermNumsSql) {
        this.outPutTermNumsSql = outPutTermNumsSql;
    }

//    public Map<String, HashSet<String>> getMediaAndOemids() {
//        return mediaAndOemids;
//    }
//
//    public void setMediaAndOemids(Map<String, HashSet<String>> mediaAndOemids) {
//        this.mediaAndOemids = mediaAndOemids;
//    }

    public String getOutPutPath() {
        return outPutPath;
    }

    public void setOutPutPath(String outPutPath) {
        this.outPutPath = outPutPath;
    }

    public int getStepSize() {
        return stepSize;
    }

    public void setStepSize(int stepSize) {
        this.stepSize = stepSize;
    }

    /**
     * 复制文件新路径下
     * @param fromPath
     * @param targetPath
     * @param fromDay
     * @param targetDay
     */
    public void copyFileToPath(String fromPath, String targetPath,String fromDay,String targetDay){

        File  fromDir = new File(fromPath);
        File [] files = fromDir.listFiles();

        File  targetDir = new File(targetPath);
        if(!targetDir.exists() || !targetDir.isDirectory()){
            targetDir.mkdir();
        }

        for(File fromFile : files){
            String fromName = fromFile.getName();
            String newName = fromName.replaceAll(fromDay,targetDay);

            String targetWholePath = targetDir + File.separator + newName;

            File targetFile =  new File(targetWholePath);

            try {
                //// TODO: 2017-9-6 验证如果文件存在是否覆盖更新？
                FileUtils.copyFile(fromFile,targetFile);
            } catch (IOException e) {
                logger.error("文件["+fromName+"]复制到["+targetFile+"]出错：",e);
            }
        }


    }

}
