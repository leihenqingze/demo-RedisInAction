package org.demo.chapter06.taskqueue;

/**
 * Created by lhqz on 2017/8/7.
 */
public class TaskQueueKeys {

    private final static String QUEUE = "queue:";

    //任务类型key
    public final static String TASKNAME = "taskName";

    //优先级高
    public final static Integer HIGH = 1;
    //优先级中
    public final static Integer MIDDLE = 2;
    //优先级低
    public final static Integer LOW = 3;

    //获取队列key
    public static String getQueueName() {
        return QUEUE + "email";
    }

    //获取对应级别的队列key
    public static String getQueueName(Integer priority) {
        return QUEUE + "email:" + priority;
    }

    //获取优先级队列的所有list
    public static String[] getQueues() {
        return new String[]{getQueueName(HIGH), getQueueName(MIDDLE), getQueueName(LOW)};
    }

}