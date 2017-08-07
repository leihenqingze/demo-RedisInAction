package org.demo.chapter06;

import org.demo.chapter06.taskqueue.*;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by lhqz on 2017/8/8.
 */
public class TaskQueueTest {

    private Jedis conn;
    private SingleTaskQueue singleTaskQueue;
    private MultipleTaskQueue multipleTaskQueue;
    private PriorityTaskQueue priorityTaskQueue;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.106");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        singleTaskQueue = new SingleTaskQueue();
        singleTaskQueue.setConn(conn);
        multipleTaskQueue = new MultipleTaskQueue();
        multipleTaskQueue.setConn(conn);
        priorityTaskQueue = new PriorityTaskQueue();
        priorityTaskQueue.setConn(conn);
    }

    @Test
    public void testSendSoldEmailViaQueue() throws InterruptedException {
        System.out.println("\n----- testSendSoldEmailViaQueue -----");
        System.out.println("Let's send a email...");
        singleTaskQueue.sendSoldEmailViaQueue("zs","itemA",20,"ls");
        assert conn.llen(TaskQueueKeys.getQueueName()) == 1;

        ProcessSoldEmailQueueThread thread = new ProcessSoldEmailQueueThread(conn);
        System.out.println("processing email...");
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        assert conn.llen(TaskQueueKeys.getQueueName()) == 0;
    }

    @Test
    public void testWorkerWatchQueue() throws InterruptedException {
        System.out.println("\n----- testWorkerWatchQueue -----");
        System.out.println("Let's send a email...");
        multipleTaskQueue.sendSoldEmailViaQueue("email","zs","itemA",20,"ls");
        assert conn.llen(TaskQueueKeys.getQueueName()) == 1;

        WorkerWatchQueueThread thread = new WorkerWatchQueueThread(conn);
        thread.addCallback("email", new Callback() {
            public void callback(String line) {
                System.out.println(line);
            }
        });
        System.out.println("processing email...");
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        assert conn.llen(TaskQueueKeys.getQueueName()) == 0;
    }

    @Test
    public void testWorkerWatchQueues() throws InterruptedException {
        System.out.println("\n----- testWorkerWatchQueues -----");
        System.out.println("Let's send a email...");
        priorityTaskQueue.sendSoldEmailViaQueue("email",2,"zs","itemA",20,"ls");
        assert conn.llen(TaskQueueKeys.getQueueName(2)) == 1;

        WorkerWatchQueueThreads thread = new WorkerWatchQueueThreads(conn);
        thread.addCallback("email", new Callback() {
            public void callback(String line) {
                System.out.println(line);
            }
        });
        System.out.println("processing email...");
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        assert conn.llen(TaskQueueKeys.getQueueName(2)) == 0;
    }

}
