package org.demo.chapter08.apistream;

import com.google.common.collect.Maps;
import redis.clients.jedis.JedisPubSub;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by lhqz on 2017/8/15.
 * 对接收的事件进行处理
 */
public class Subscriber extends JedisPubSub {

    /**
     * 客户端标识队列
     */
    private final Map<OutputStream, List<Filter>> outputStreams = Maps.newHashMap();

    /**
     * 当接受到事件时进行处理
     * @param channel 订阅的主题
     * @param message 事件消息
     */
    @Override
    public void onMessage(String channel, String message) {
        byte[] bytes = message.getBytes();
        if (null != outputStreams && outputStreams.size() > 0) {
            for (Map.Entry<OutputStream, List<Filter>> outputStream : outputStreams.entrySet()) {
                boolean bool = true;
                if (null != outputStream.getValue() && outputStream.getValue().size() > 0) {
                    for (Filter filter : outputStream.getValue()) {
                        if (filter.filter(message)) {
                            bool = false;
                            break;
                        }
                    }
                    if (bool) {
                        BufferedOutputStream bufferedOutputStream = null;
                        try {
                            bufferedOutputStream = new BufferedOutputStream(outputStream.getKey());
                            bufferedOutputStream.write(bytes);
                        } catch (IOException e) {
                            e.printStackTrace();
                            //出现异常时候断开客户端
                            try {
                                outputStream.getKey().flush();
                                outputStream.getKey().close();
                                outputStreams.remove(outputStream.getKey());
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        } finally {
                            try {
                                bufferedOutputStream.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 将某个客户端加入进来
     * @param outputStream 客户端
     * @param filters 事件过滤器
     */
    public void add(OutputStream outputStream, List<Filter> filters) {
        outputStreams.put(outputStream, filters);
    }

}