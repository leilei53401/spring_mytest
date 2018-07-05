package com.voole.ad.prepare.detail;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.objectweb.asm.tree.TryCatchBlockNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * 流量优化计算终端个数
 * 扫描媒体排期表，计算需要生成的终端个数
 * @author shaoyl
 */
public class FlowOptCalTerminalNumJob {

    Logger logger = Logger.getLogger(FlowOptCalTerminalNumJob.class);
//	private String oemids; //需要过滤的渠道ids，多个逗号隔开
	private String scanSql;//查询媒体排期sql语句
    private String isCalSql; //计算记录表
    private String areaControlSql; //区域限定信息
    private String areaRateSql; //区域占比信息
    private float defaultFreq;
    private String insertCalStatusSql;
    private String insertTermNumSql;

    @Autowired
	public JdbcTemplate adSupersspJt;

	public Map<String,HashSet<String>> calTerminalNums() {
		logger.info("--------------进入需生成终端个数计算-------------------------");

        //过滤媒体排期表
        DateTime dt = new DateTime();
        String currDt = dt.toString("yyyy-MM-dd HH:mm:ss");

		String newSql = scanSql.replaceAll("@time", currDt);
//        newSql = newSql.replaceAll("@oemid",oemids);

		logger.info("scanSql 替换后为："+newSql);
		//查询有效排期信息,限定oemid
        List<Map<String,Object>> medaiPlanInfoList =  adSupersspJt.queryForList(newSql);
        //需要计算或者重新计算的媒体(多个排期时，创建排期时间不同，则可能需要重新计算终端)
//        HashSet<String> mediaReCalSet = new HashSet<String>();
         //Map<mediaid,HashSet<oemid>> , 一个媒体对应多个 oemid 的情况
        Map<String,HashSet<String>> mediaReCalMap = new HashMap<String,HashSet<String>>();

        if(null!=medaiPlanInfoList && medaiPlanInfoList.size()>0){

            logger.info("---------------获取到需要流量优化的排期【"+medaiPlanInfoList.size()+"】条!------------------------");

            //查询是否计算信息表
            String dayTime = dt.toString("yyyyMMdd");
            String isCalSqlNew = isCalSql.replaceAll("@daytime",dayTime);

            List<Map<String,Object>> isCalInfoList =  adSupersspJt.queryForList(isCalSqlNew);

            Map<String,Map<String,Object>> isCalMapInfo = forMatCalInfoToMap(isCalInfoList);

            //每个排期是否计算的状态
            List<Map<String,String>> calStatusList = new ArrayList<Map<String,String>>();
            //每个排期下，每个区域，生成的终端个数
            List<Map<String,String>> termNumList = new ArrayList<Map<String,String>>();

            //注：个数计算对应到排期上
            //但输出结果计算只对应到媒体上即可。(相同媒体，多个排期，相同区域累加)

            for(Map<String,Object> mediaPlan:medaiPlanInfoList){

                String mediaPlanid = mediaPlan.get("media_planid").toString();
                String mediaid = mediaPlan.get("mediaid").toString();
                String oemid  = mediaPlan.get("oemid").toString();

                //限定曝光次数
                long amount = Long.valueOf(mediaPlan.get("amount").toString());

                //TODO:是否限定频次
               /* float frequencyValue = 2.5F;
                String frequencyflag = mediaPlan.get("frequencyflag").toString();
                
                if("1".equals(frequencyflag)){
                    //// TODO: 2017-8-24 获取限定频次的具体值
                }else{
                    frequencyValue  = defaultFreq;
                }*/

                float frequencyValue  = defaultFreq;

                //此处要生成的终端数
                int terminalNums = (int) (amount/frequencyValue);

                //########### 区域限定情况 #############
                String area_flag = mediaPlan.get("area_flag").toString();


                //查看此排期是否已计算过要输出的终端量
                Map<String,Object> isCalData = isCalMapInfo.get(mediaPlanid + "_" + dayTime);

                if(null== isCalData || Integer.valueOf(isCalData.get("iscal").toString()) != 1){
                    //存在未计算数据, 输出数据需重新计算
                    //记录mediaid 和 oemid
//                    mediaReCalSet.add(mediaid);
                    logger.info("########## 开始计算 mediaid = ["+mediaid+"], oemid=["+oemid+"], mediaPlanid=["+mediaPlanid+"], daytime=["+dayTime+"] 的终端数量. ################");
                    HashSet<String> oemidSet = mediaReCalMap.get(mediaid);

                    if(null!=oemidSet){
                        //已存在，增加的新oemid
                        oemidSet.add(oemid);
                    }else{
                        HashSet<String> tmpSet  = new HashSet<>();
                        tmpSet.add(oemid);
                        mediaReCalMap.put(mediaid,tmpSet);
                    }

                    //未计算,根据排期计算终端数据
                     if("1".equals(area_flag)){
                         //限定区域
                         //根据媒体排期编号查询限定区域
                         logger.info("mediaPlanid = ["+mediaPlanid+"] 限定区域");
                         String newAreaSql = areaControlSql.replaceAll("@mediaplanid",mediaPlanid);
                         logger.info("查询区域限定sql为:"+newAreaSql);
                         List<Map<String,Object>> areaControlList = adSupersspJt.queryForList(newAreaSql);
                         if(null!=areaControlList && areaControlList.size()>0){
                             logger.info("查询出区域限定数据为["+areaControlList.size()+"]条!");
                         }else{
                             logger.warn("未获取到区域限定数据，请核查！");
                         }

                         //查询要分配的占比
                         String newRateSql = areaRateSql.replaceAll("@mediaid",mediaid);
                         logger.info("查询要分配占比sql为:"+newRateSql);
                         List<Map<String,Object>> areaRateList = adSupersspJt.queryForList(newRateSql);

                         Map<String,String> areaRateMap = null;
                        if(null!=areaRateList && areaRateList.size()>0){
                            logger.info("查询出媒体【"+mediaid+"】区域占比数据为["+areaRateList.size()+"]条!");
                             areaRateMap =  this.formatAreaRateList(areaRateList);
                            logger.info("转化后媒体【"+mediaid+"】区域占比数据为["+areaRateMap.size()+"]条!");
                        }else{
                            logger.warn("未获取到媒体【"+mediaid+"】的占比数据!");
                        }


                         //累加占比总数
                         float sum=0L;

                         if(null != areaRateMap && areaRateMap.size()>0) {
                             for(Map<String,Object> areaControlInfo:areaControlList){
                                 String area_code = areaControlInfo.get("area_code").toString();
                                 String key = mediaid + "_" + area_code;
                                 float termNum = 0L;

                                 try {
                                     termNum = Long.valueOf((null==areaRateMap.get(key))?"0":areaRateMap.get(key));
                                 }catch (Exception e){
                                     logger.warn("获取区域占比key=["+key+"]的数据异常：",e);
                                     termNum = 0L;
                                 }
                                 sum += termNum;
                             }
                         }

                         //计算终端个数
                        if(sum>0L){
                            for(Map<String,Object> areaControlInfo:areaControlList){
                                String area_code = areaControlInfo.get("area_code").toString();
                                String key = mediaid + "_" + area_code;

                                float termNum = 0F;

                                try {
                                    termNum = Long.valueOf((null==areaRateMap.get(key))?"0":areaRateMap.get(key));
                                }catch (Exception e){
                                    logger.warn("获取区域占比key=["+key+"]的数据异常：",e);
                                    termNum = 0F;
                                }
//                                float termNum = Long.valueOf(areaRateMap.get(key));

                                long areaTermNum = (long)(terminalNums * (termNum/sum));

                                //定义存储计算结果
                                Map<String,String> result = new HashMap<>();
                                result.put("daytime",dayTime);
                                result.put("mediaid",mediaid);
                                result.put("oemid",oemid);
                                result.put("mediaPlanid",mediaPlanid);
                                result.put("area_code",area_code);
                                result.put("areaTermNum",areaTermNum+"");

                                termNumList.add(result);
                            }
                        }else{
                            logger.warn("累加占比数据错误：sum = ["+sum+"]");
                        }


                     }else {
                         //不限定区域
                        //查询要分配的占比,直接拿占比计算结果
                         logger.info("mediaPlanid = ["+mediaPlanid+"] 不限区域!");
                         String newRateSql = areaRateSql.replaceAll("@mediaid",mediaid);
                         List<Map<String,Object>> areaRateList = adSupersspJt.queryForList(newRateSql);

                         if(null!=areaRateList && areaRateList.size()>0) {

                             logger.warn("取到媒体【"+mediaid+"】的占比数据["+areaRateList.size()+"]条!");

                             for (Map<String, Object> data : areaRateList) {

                                 String area_code = data.get("area_code").toString();
                                 float proportion = Long.valueOf(data.get("proportion").toString());


                                 long areaTermNum = (long) (terminalNums * (proportion / 100000f));

                                 //定义存储计算结果
                                 Map<String, String> result = new HashMap<>();
                                 result.put("daytime", dayTime);
                                 result.put("mediaid", mediaid);
                                 result.put("oemid", oemid);
                                 result.put("mediaPlanid", mediaPlanid);
                                 result.put("area_code", area_code);
                                 result.put("areaTermNum", areaTermNum + "");
                                 termNumList.add(result);
                             }
                         }
                         else{
                             logger.warn("未获取到媒体【"+mediaid+"】的占比数据!");
                         }
                     }

                    //记录计算状态
                    Map<String,String> calStatusResult = new HashMap<String,String>();
                    calStatusResult.put("daytime",dayTime);
                    calStatusResult.put("mediaid",mediaid);
                    calStatusResult.put("oemid",oemid);
                    calStatusResult.put("planid",mediaPlanid);
                    calStatusResult.put("iscal","1");

                    calStatusList.add(calStatusResult);

                    logger.info("########## 结束计算 mediaid = ["+mediaid+"], oemid=["+oemid+"], mediaPlanid=["+mediaPlanid+"], daytime=["+dayTime+"] 的终端数量. ################");


                }else{
                    logger.info("########## mediaid = ["+mediaid+"] , oemid=["+oemid+"],  mediaPlanid=["+mediaPlanid+"], daytime=["+dayTime+"] 需要终端数量已计算. ################");
                }
            }

            //记录计算状态
            if(null!= calStatusList && calStatusList.size()>0){
                insertCalStatus(calStatusList);
            }else{
                logger.info("本次任务没有要记录的计算状态！");
            }

            //将计算需要的终端个数结果保存到表中
            if(null!=termNumList && termNumList.size()>0){
                insertTermNumsBatch(termNumList);
            }else {
                logger.info("本次任务没有要记录的终端计算结果！");
            }

        }else{
            logger.info("---------------未获取到需要流量优化的排期!------------------------");
        }


		long end  = System.currentTimeMillis();

        logger.info("--------------需要计算的媒体和渠道为 "+mediaReCalMap.toString()+"------------------------");
        logger.info("---------------结束生成终端个数计算任务!------------------------");

        return  mediaReCalMap;
	}

/*
    public String getOemids() {
        return oemids;
    }


    public void setOemids(String oemids) {
        this.oemids = oemids;
    }*/

    public String getScanSql() {
        return scanSql;
    }

    public void setScanSql(String scanSql) {
        this.scanSql = scanSql;
    }

    public String getIsCalSql() {
        return isCalSql;
    }

    public void setIsCalSql(String isCalSql) {
        this.isCalSql = isCalSql;
    }

    public float getDefaultFreq() {
        return defaultFreq;
    }

    public void setDefaultFreq(float defaultFreq) {
        this.defaultFreq = defaultFreq;
    }

    public String getAreaControlSql() {
        return areaControlSql;
    }

    public void setAreaControlSql(String areaControlSql) {
        this.areaControlSql = areaControlSql;
    }

    public String getAreaRateSql() {
        return areaRateSql;
    }

    public void setAreaRateSql(String areaRateSql) {
        this.areaRateSql = areaRateSql;
    }

    public String getInsertCalStatusSql() {
        return insertCalStatusSql;
    }

    public void setInsertCalStatusSql(String insertCalStatusSql) {
        this.insertCalStatusSql = insertCalStatusSql;
    }

    public String getInsertTermNumSql() {
        return insertTermNumSql;
    }

    public void setInsertTermNumSql(String insertTermNumSql) {
        this.insertTermNumSql = insertTermNumSql;
    }

    /**
     * 将list结构数据转化成map结构
     * @param isCalInfoList
     * @return
     */
    private Map<String,Map<String,Object>> forMatCalInfoToMap(List<Map<String,Object>> isCalInfoList){
        Map<String,Map<String,Object>> isCalMap =  new HashMap<String,Map<String,Object>>();
        for(Map<String,Object> data:isCalInfoList){

            String daytime = data.get("daytime").toString();
            String planid = data.get("planid").toString();
            String key = planid+"_"+daytime;
            isCalMap.put(key,data);

        }
        return isCalMap;
    }

    /**
     * 将 区域占比转化为Map格式
     * @param areaRateList
     * @return
     */
    private Map<String,String> formatAreaRateList(List<Map<String,Object>> areaRateList){
        Map<String,String> ratMap =  new HashMap<String,String>();
        for(Map<String,Object> data:areaRateList){

            String media_id = data.get("media_id").toString();
            String area_code = data.get("area_code").toString();
            String proportion = data.get("proportion").toString();
            String key = media_id+"_"+area_code;
            ratMap.put(key,proportion);

        }
        return ratMap;
    }

    /**
     *  记录计算结果状态
     * @param calStatusList
     */
    public void insertCalStatus(final List<Map<String,String>> calStatusList){

            int[] result = adSupersspJt.batchUpdate(insertCalStatusSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String,String> calStatusData = calStatusList.get(i);
                ps.setString(1, calStatusData.get("daytime"));
                ps.setString(2, calStatusData.get("mediaid"));
                ps.setString(3, calStatusData.get("oemid"));
                ps.setString(4, calStatusData.get("planid"));
                ps.setString(5, calStatusData.get("iscal"));
            }

            @Override
            public int getBatchSize() {
                return calStatusList.size();
            }
        });

        logger.info("记录计算状态["+result.length+"]条!");
    }

    /**
     *  计算需要的终端个数结果保存到表中
     * @param terminalnums
     */
    public void insertTermNumsBatch(final List<Map<String,String>> terminalnums){

      /*  String sql = "INSERT INTO FLOWOPT_TERMINAL_NUMS " +
                "(DAYTIME, MEDIAID, OEMID, PLANID, AREA_CODE, TERMIAL_NUM) VALUES (?, ?, ?, ?, ?, ?)";*/

        int[] result = adSupersspJt.batchUpdate(insertTermNumSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String,String> terminalNumsData = terminalnums.get(i);
                ps.setString(1, terminalNumsData.get("daytime"));
                ps.setString(2, terminalNumsData.get("mediaid"));
                ps.setString(3, terminalNumsData.get("mediaid")+"101");
                ps.setString(4, terminalNumsData.get("mediaPlanid"));
                ps.setString(5, terminalNumsData.get("area_code"));
                ps.setString(6, terminalNumsData.get("areaTermNum"));
            }

            @Override
            public int getBatchSize() {
                return terminalnums.size();
            }
        });

        logger.info("记录计算区域终端个数["+result.length+"]条!");
    }
}
