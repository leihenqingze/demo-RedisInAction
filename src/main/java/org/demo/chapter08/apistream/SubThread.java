package org.demo.chapter08.apistream;

import lombok.AllArgsConstructor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by lhqz on 2017/8/15.
 * 使用一个新的线程进行事件监听
 */
@AllArgsConstructor
public class SubThread extends Thread {

    private final JedisPool jedisPool;
    private final Subscriber subscriber;

    @Override
    public void run() {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(subscriber, ApiStream.channel);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.close();
        }
    }
}