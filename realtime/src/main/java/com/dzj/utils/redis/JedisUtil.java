package com.dzj.utils.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.dzj.common.RedisConfig;

/**
 * Author: zhangly
 * Date: 2023/4/25 19:06
 * FileName: JedisUtil
 * Description: 连接redis
 */
public class JedisUtil {
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
        jedispool = new JedisPool(jedisPoolConfig, RedisConfig.REDIS_HOST, RedisConfig.REDIS_PORT, RedisConfig.REDIS_TIMEOUT, RedisConfig.REDIS_PASSWORD);

    }

    public static Jedis getJedis() {
        return jedispool.getResource();
    }
}
