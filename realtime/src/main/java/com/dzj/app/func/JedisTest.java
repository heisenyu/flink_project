package com.dzj.app.func;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


/**
 * Author: zhangly
 * Date: 2023/4/25 18:45
 * FileName: JedisTest
 * Description: jedisTest
 */
public class JedisTest {
    private static final JedisPool jedispool;
    static {
//        配置连接池
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//        配置最大连接数
        jedisPoolConfig.setMaxTotal(8);
        jedisPoolConfig.setMaxIdle(8);
//        保留一定的空闲连接池对象；
        jedisPoolConfig.setMinIdle(0);
//        等待时间
        jedisPoolConfig.setMaxWaitMillis(1000);
//        创建连接池对象
        jedispool = new JedisPool(jedisPoolConfig,"172.29.28.187",6379,1000,"123456");

    }
    public static Jedis getJedis(){
        return jedispool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
/*//        指定库
        jedis.select(0);
//       存入数据
        long hmset = jedis.hset("user:1", "name", "Jack1");
        System.out.println(hmset);
        Map<String, String> stringStringMap = jedis.hgetAll("user:1");
        System.out.println(stringStringMap);*/
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            System.out.println(jedis.get(key));
        }

    }


}
