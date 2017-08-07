package org.demo.chapter06.taskqueue;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by lhqz on 2017/8/8.
 * 获取任务并执行处理
 * 因为工作进程除了发送邮件之外不需要执行其他工作,
 * 所以它将使用阻塞版的弹出命令BLPOP从队中弹出待发送的邮件
 */
public class ProcessSoldEmailQueueThread extends Thread {

    private Jedis conn;
    private boolean quit;

    public ProcessSoldEmailQueueThread(Jedis conn) {
        this.conn = conn;
    }

    public void quit() {
        quit = true;
    }

    @Override
    public void run() {
        while (!quit) {
            //尝试从队里里面取出一项待执行任务
            List<String> packeds = conn.blpop(30, TaskQueueKeys.getQueueName());
            //队列不为空,对任务进行处理
            if (null != packeds && packeds.size() > 1) {
                String packed = packeds.get(1);
                try {
                    //模拟任务处理
                    System.out.println(packed);
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