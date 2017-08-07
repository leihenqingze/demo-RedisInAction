package org.demo.chapter06.taskqueue;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * Created by lhqz on 2017/8/8.
 * 多任务队列处理类
 * 因为BLPOP命令每次只会从队列里面弹出一项任务,
 * 所以任务不会重复出现,也不会被重复处理.并且因为队列只会存放待处理任务,
 * 所以工作进程要处理的任务时非常单一的.
 * <p>
 * 可以通过为每种类型的任务使用不同的list,但是如果一个list能处理多种类型
 * 的任务,那么事情会方便很多.这里使用的是单个list处理多种类型的任务.
 */
public class WorkerWatchQueueThread extends Thread {

    private Jedis conn;
    private boolean quit;
    private Map<String, Callback> callbackMap = Maps.newConcurrentMap();

    public WorkerWatchQueueThread(Jedis conn) {
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
            List<String> packeds = conn.blpop(30, TaskQueueKeys.getQueueName());
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