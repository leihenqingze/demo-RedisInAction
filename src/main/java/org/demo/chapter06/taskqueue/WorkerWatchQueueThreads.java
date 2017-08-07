package org.demo.chapter06.taskqueue;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * Created by lhqz on 2017/8/8.
 * 多任务优先级队列处理类
 * 这里模拟了高、中、低上个级别的优先级队列
 * 同时使用多个list可以降低实现优先级特性的难度.
 * <p>
 * Resque是Ruby语言基于Redis实现的队列库
 */
public class WorkerWatchQueueThreads extends Thread {

    private Jedis conn;
    private boolean quit;
    private Map<String, Callback> callbackMap = Maps.newConcurrentMap();

    public WorkerWatchQueueThreads(Jedis conn) {
        this.conn = conn;
    }

    public void quit() {
        quit = true;
    }

    public void addCallback(String key, Callback callback) {
        callbackMap.put(key, callback);
    }

    @Override
    public void run() {
        while (!quit) {
            //尝试从队里里面取出一项待执行任务
            //BLPOP从多个队列中弹出时,按照从左到右的方式获取,如果左边没有元素,才会从右边中弹出
            List<String> packeds = conn.blpop(30, TaskQueueKeys.getQueues());
            //队列不为空,对任务进行处理
            if (null != packeds && packeds.size() > 1) {
                String packed = packeds.get(1);
                //根据任务类型获取处理函数
                Map<String, String> json = (Map<String, String>) JSON.parse(packed);
                try {
                    //模拟任务处理
                    Callback callback = callbackMap.get(json.get(TaskQueueKeys.TASKNAME));
                    callback.callback(packed);
                    System.out.println("处理成功");
                } catch (Exception e) {
                    System.out.println("处理失败");
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}