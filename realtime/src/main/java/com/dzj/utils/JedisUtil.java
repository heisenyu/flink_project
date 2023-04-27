package com.dzj.utils;

import com.dzj.common.ComConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class JedisUtil {
    private static JedisPool jedisPool;

    private static void initJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);


        jedisPool = new JedisPool(poolConfig, ComConfig.REDIS_HOST, ComConfig.REDIS_PORT, 10000, ComConfig.REDIS_PASSWORD);

    }

    public static Jedis getJedis() {
        if (jedisPool == null) {
            initJedisPool();
        }
        // 获取Jedis客户端
        Jedis jedis = jedisPool.getResource();
//        jedis.auth(ComConfig.REDIS_PASSWORD);
        return jedis;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();

        //获取所有的key
        System.out.println("获取所有的key");
        System.out.println(jedis.keys("*"));

//        System.out.println("清空数据：" + jedis.flushDB());


//        String pong = jedis.ping();
//        jedis.hset("test", "name", "dzj");
//        jedis.hset("test", "age", "18");
//        HashMap<String, String> map = new HashMap<>();
//        map.put("name", "dzj02");
//        map.put("age", "1802");
//        jedis.hset("test02", map);
//        System.out.println(pong);
//
//        System.out.println(jedis.hget("test", "name"));
//        System.out.println(jedis.hget("test", "age"));
//        System.out.println(jedis.hgetAll("test02"));
        jedis.close();
    }

}