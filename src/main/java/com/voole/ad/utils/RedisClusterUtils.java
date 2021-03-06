package com.voole.ad.utils;

/**
 * Created by shaoyl on 2018-5-25.
 */
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Set;

@Component
public class RedisClusterUtils {

    @Autowired
    private JedisCluster jedisCluster;

    /**
     * 得到指定key值的value
     * @param key
     */
    public String get(String key){
        return jedisCluster.get(key);
    }

    /**
     * 保存指定key值的value
     * @param key
     * @param value
     */
    public void set(String key, String value){
        jedisCluster.set(key, value);
    }

    /**
     * 保存指定key值的value
     * @param key
     * @param list
     */
    public void set(String key, List<String> list){
        jedisCluster.rpush(key, (String[]) list.toArray());
    }

    /**
     * 删除指定key的value
     * @param key
     */
    public void del(String key){
        jedisCluster.del(key);
    }

    /**
     * 获取set结构所有成员
     * @param key
     * @return
     */
    public Set<String> smembers(String key) {
        return  jedisCluster.smembers(key);
    }

    public Long scard(String key) {
        return  jedisCluster.scard(key);
    }

    /**
     * 删除set中key
     * @param key
     * @param member
     * @return
     */
    public Long srem(String key, String... member) {
        return jedisCluster.srem(key,member);
    }

}