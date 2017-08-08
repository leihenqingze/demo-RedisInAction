package org.demo.chapter06.delaytaskqueue;

/**
 * Created by lhqz on 2017/8/8.
 * 延迟任务相关key
 */
public class Keys {

    //延迟任务集合key
    public final static String DELAYED = "delayed:";

    //获取队列key
    public static String getQueueKey(String queue){
        return "queue:" + queue;
    }

}
