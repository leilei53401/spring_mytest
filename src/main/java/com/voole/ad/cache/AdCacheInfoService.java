package com.voole.ad.cache;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author shaoyl
 * 缓存区域地市省份关系
 */
@Service
public class AdCacheInfoService implements InitializingBean {

	private Logger logger = LoggerFactory.getLogger(AdCacheInfoService.class);

	private final Map<String,String> areaRelationMap;//渠道媒体映射关系
    //默认广协IP库缓存
    private  final RangeMap<Double, String[]> defaultIpRangeMap;

	private final ReentrantReadWriteLock readWriteLock;
	private final Lock read;
	private final Lock write;


    @Autowired
    public JdbcTemplate adSupersspJt;
	
	private DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
    public AdCacheInfoService() {

    	//渠道和媒体
        areaRelationMap  = new HashMap<String,String>();

        //IP地址库缓存初始化
        defaultIpRangeMap =  TreeRangeMap.create();;

		// lock
		readWriteLock = new ReentrantReadWriteLock();
		read = readWriteLock.readLock();
		write = readWriteLock.writeLock();
	}

    
    /**
     * 缓存地市和省份关系
     */
    public void doAreaRelationCache(){
        String areaSql="SELECT c.area_code cityid, p.area_code provinceid" +
                " FROM super_ssp.area c , super_ssp.area p" +
                " WHERE c.area_type = 2 AND p.area_type=1" +
                " AND c.parent_id = p.area_code";

		Map<String, String> areaMapper = new HashMap<String, String>();
		try {
			List<Map<String, Object>> oemMediaMapperList = adSupersspJt.queryForList(areaSql);

			for (Map<String, Object> map : oemMediaMapperList) {
                String cityid = map.get("cityid")+"";
				String provinceid = map.get("provinceid")+"";
                areaMapper.put(cityid, provinceid);
			}
			
			logger.info("区域映射关系加载完成！加载【" + areaMapper.size() + "】条！");
			logger.debug("区域映射关系加载完成！加载明细：" + areaMapper.toString());

		} catch (Exception e) {
			logger.error("加载区域映射关系出错 :", e);
			return;
		}

		// 更新缓存
		if(null!=areaMapper && areaMapper.size()>0){
			logger.info("areaRelationMap cache updated start ... ");
			try {
				write.lock();
				this.areaRelationMap.clear();
				this.areaRelationMap.putAll(areaMapper);
				logger.info("areaRelationMap cache updated ... size = ["
						+ areaRelationMap.size() + "]");
			} catch (Exception e) {
				logger.warn("oemMediaRelationMap doRefresh error", e);
			} finally {
				write.unlock();
			}	
			logger.info("areaRelationMap cache updated end ... ");
		}    	
    }


    /**
     * 缓存广协ip地址库数据
     */
    public void doAreaInfoRefresh() {
        logger.info("AreaInfoReCache doRefresh start....");
//        logger.debug("where is Range.class : "+ClassLocationUtils.where(Range.class));

        String ipSql = "select a.parent_id provinceid,a.area_code cityid,p.netseg1dec,p.netseg2dec" +
                " from  super_ssp.ad_ip_topology p, super_ssp.area a " +
                " where a.area_type=2 and p.adcode = a.area_code";

        RangeMap<Double, String[]> rangeMap = TreeRangeMap.create();
        final AtomicLong at=new AtomicLong(0l);
        try {
//            List<Map<String, Object>> areaMapperList = cacheDao.loadAreaMapperInfo();

            List<Map<String, Object>> areaMapperList = adSupersspJt.queryForList(ipSql);
            for (Map<String, Object> map : areaMapperList) {

//				logger.debug("mapdata is : "+map);

                String[] data = { map.get("provinceid")+"",
                        map.get("cityid")+"" };
                double netseg1dec =  (Double)map.get("netseg1dec");
                double netseg2dec = (Double)map.get("netseg2dec");
                if (netseg1dec <= netseg2dec) {
                    rangeMap.put(Range.closed(netseg1dec, netseg2dec), data);
                }

                at.getAndIncrement();
            }

            logger.info("加载广协IP地址库【"+at.get()+"】条!");
        } catch (Exception e) {
            logger.error("加载ip区域异常", e);
            return;
        }

        //更新缓存
        if(null!=rangeMap && at.get()>0){
            logger.info(" ip database cache update start ... !");
            try{
                write.lock();
                this.defaultIpRangeMap.clear();
                this.defaultIpRangeMap.putAll(rangeMap);

                logger.info(" ip database cache updated ... !");

            } catch (Exception e) {
                logger.warn("ip database cache doRefresh error", e);
            } finally {
                write.unlock();
            }
            logger.info(" ip database cache update end ... !");
        }
        logger.info("AreaInfoReCache doRefresh end....");

    }
    

    
    
	//启动初始化缓存
	@Override
	public void afterPropertiesSet() {
		logger.info("doAreaRelationCache init start....");

		doAreaRelationCache();
        doAreaInfoRefresh();

        logger.info("doAreaRelationCache init end....");
	}
	

	
	/**
	 * 通过渠道id获取媒体id
	 * @param cityid
	 * @return
	 */
	public String getProvinceIdByCityId(String cityid){
		String provinceid = "0";//默认
		if(StringUtils.isNotBlank(this.areaRelationMap.get(cityid))){
            provinceid = this.areaRelationMap.get(cityid);
		}else{
			logger.warn("未获取到 cityid=["+cityid+"] 的省编号！return default provinceid=["+provinceid+"]");
		}
		return provinceid;
	}


    /**
     * 根据厂家和IP获取地域Id
     * @param ip
     * @return Integer[] data, data[0] : 省份code， data[1]:地市code
     */
    public String[] getAreaInfo(String ip) {
        String[] areaData = {"0","0"};
        if(StringUtils.isBlank(ip)){
            logger.warn("IP IS NULL");
            return areaData;
        }
        double longIp = Double.valueOf(ipToDecimal(ip)+"");
        read.lock();
        try {
            areaData = defaultIpRangeMap.get(longIp);
        } catch (Exception e) {
            logger.warn("get ip area error:", e);
        } finally {
            read.unlock();
        }

        if(null==areaData){
            areaData = new String[2];
            areaData[0]="0";
            areaData[1]="0";
        }

        return areaData;
    }


    /**
     * 将IP转化为十进制
     * @param ip
     * @return
     */
    private  long ipToDecimal(String ip) {
        long ipDec = 0;
        if (ip != null) {
            String[] ipArr = ip.split("\\.");
            if (ipArr != null && ipArr.length == 4) {
                for (int i = 3; i >= 0; i--) {
                    ipDec += (Long.valueOf(ipArr[i].trim()) * Math.pow(256, 3 - i));
                }
            }
        }
        return ipDec;
    }
	

}
