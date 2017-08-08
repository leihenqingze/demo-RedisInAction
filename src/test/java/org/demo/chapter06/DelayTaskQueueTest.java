package org.demo.chapter06;

import org.apache.commons.lang3.StringUtils;
import org.demo.chapter06.delaytaskqueue.DelayTaskQueue;
import org.demo.chapter06.delaytaskqueue.PollQueueThread;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;

/**
 * Created by lhqz on 2017/8/8.
 */
public class DelayTaskQueueTest {

    private Jedis conn;
    private DelayTaskQueue queue;
    private DistributedLock lock;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.106");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        lock = new DistributedLock();
        lock.setConn(conn);
        queue = new DelayTaskQueue();
        queue.setConn(conn);
    }

    @Test
    public void testDelayedTasks() throws InterruptedException {
        System.out.println("\n----- testDelayedTasks -----");
        System.out.println("Let's start some regular and delayed tasks...");
        for (long delay : new long[]{0,500,0,1500}) {
            assert StringUtils.isNotEmpty(queue.executeLater("tqueue","testfn", new ArrayList<String>(),delay));
        }
        long r = conn.llen("queue:tqueue");
        System.out.println("How many non-delayed tasks are there (should be 2)? " + r);
        assert r == 2;
        System.out.println();

        System.out.println("Let's start up a thread to bring those delayed tasks back...");
        PollQueueThread thread = new PollQueueThread(conn,lock);
        thread.start();
        System.out.println("Started:");
        System.out.println("Let's wait for those tasks to be prepared...");
        Thread.sleep(2000);
        thread.quit();
        thread.join();
        r = conn.llen("queue:tqueue");
        System.out.println("Waiting is over,how many tasks do we have (should be 4)? " + r);
        assert r == 4;
    }

}