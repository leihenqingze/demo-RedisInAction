package org.demo.chapter06;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by lhqz on 2017/8/5.
 */
public class DistributedLockTest {

    private Jedis conn;
    private DistributedLock lock;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        lock = new DistributedLock();
        lock.setConn(conn);
    }

    @Test
    public void testDistributedLocking() throws InterruptedException {
        System.out.println("\n----- testDistributedLocking -----");
        System.out.println("Getting an initial lock...");
        assert StringUtils.isNotEmpty(lock.acquireLockWithTimeout("testlock",1000,1000));
        System.out.println("Got it!");
        System.out.println("Trying to get it again without releasing the first one...");
        assert StringUtils.isEmpty(lock.acquireLockWithTimeout("testlock",10,1000));
        System.out.println("Failed to get it!");
        System.out.println();

        System.out.println("Waiting for the lock to timeout...");
        Thread.sleep(2000);
        System.out.println("Getting the lock again...");
        String lockId = lock.acquireLockWithTimeout("testlock",1000,1000);
        assert StringUtils.isNotEmpty(lockId);
        System.out.println("Got it!");
        System.out.println("Releasing the lock...");
        assert lock.releaseLock("testlock",lockId);
        System.out.println("Released it...");
        System.out.println();

        System.out.println("Acquiring it again...");
        assert StringUtils.isNotEmpty(lock.acquireLockWithTimeout("testlock",1000,1000));
        System.out.println("Got it!");

    }

    @After
    public void destroy() {
        conn.flushDB();
    }

}