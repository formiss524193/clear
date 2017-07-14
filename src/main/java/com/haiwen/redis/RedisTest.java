package com.haiwen.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lenovo on 2017/7/12.
 */
public class RedisTest {
    public static void main(String[] args) {
//        Set<HostAndPort> jedisNodes=new HashSet<HostAndPort>();
//        jedisNodes.add(new HostAndPort("192.168.0.154",6379));
          Jedis jedis=new Jedis("192.168.0.152");
//        jedis.auth("admin");
      //  jedis.re("liuchang");
        Map<String, String> map=jedis.hgetAll("liuchang1");
        jedis.zadd("liuc123",2,"192.168.0.152");
        for (String key:map.keySet()) {
            System.out.println(map.get(key));
        }
        System.out.print(map.toString());

    }
}
