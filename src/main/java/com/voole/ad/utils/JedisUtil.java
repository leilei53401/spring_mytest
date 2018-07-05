package com.voole.ad.utils;

/**
 * Created by Administrator on 2018-5-21.
 */
import redis.clients.jedis.Jedis;

import java.util.Set;

public class JedisUtil {

    private static final String SERVER_ADDRESS = "192.168.2.5";	//服务器地址
    private static final Integer SERVER_PORT = 6379 ;	//端口

    private static Jedis jedis ;

    static {
        jedis = new Jedis(SERVER_ADDRESS, SERVER_PORT);
    }


    public static String get(String key) {
        return jedis.get(key);
    }

    public static Set<String> smembers(String key) {
        return jedis.smembers(key);
    }

    public static Long scard(String key){
        return  jedis.scard(key);
    }




}
