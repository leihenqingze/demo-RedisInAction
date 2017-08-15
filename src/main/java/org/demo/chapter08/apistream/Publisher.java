package org.demo.chapter08.apistream;

import lombok.AllArgsConstructor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by lhqz on 2017/8/15.
 * 模拟事件产生
 */
@AllArgsConstructor
public class Publisher {

    private final JedisPool jedisPool;

    public void start() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Jedis jedis = jedisPool.getResource();
        while (true) {
            String line = null;
            try {
                line = reader.readLine();
                if (!"quit".equals(line)) {
                    //产生事件
                    jedis.publish(ApiStream.channel, line);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
