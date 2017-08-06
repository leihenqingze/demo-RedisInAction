package org.demo.chapter06;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by lhqz on 2017/8/5.
 */
public class SemaphoreTest {

    private Jedis conn;
    private Semaphore semaphore;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        semaphore = new Semaphore();
        semaphore.setConn(conn);
        DistributedLock lock = new DistributedLock();
        lock.setConn(conn);
        semaphore.setLock(lock);
    }

    @Test
    public void testCountingSemaphore() throws InterruptedException {
        System.out.println("\n----- testCountingSemaphore -----");
        System.out.println("Getting 3 initial semaphores with a limit of 3...");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNoneEmpty(semaphore.acquireSemaphore("testsem", 3, 1000));
        }
        System.out.println("Done!");
        System.out.println("Getting one more that should fail...");
        assert StringUtils.isEmpty(semaphore.acquireSemaphore("testsem", 3, 1000));
        System.out.println("Couldn't get it!");
        System.out.println();

        System.out.println("Lets's wait for some of them to time out");
        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = semaphore.acquireSemaphore("testsem", 3, 1000);
        assert StringUtils.isNotEmpty(id);
        System.out.println("Got one!");
        System.out.println("Let's release it...");
        assert semaphore.releaseSemaphore("testsem", id);
        System.out.println("Released!");
        System.out.println();
        System.out.println("And let's make sure we can get 3 more");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNotEmpty(semaphore.acquireSemaphore("testsem", 3, 1000));
        }
        System.out.println("We got them!");
    }

    @Test
    public void testAcquireFairSemaphore() throws InterruptedException {
        System.out.println("\n----- testAcquireFairSemaphore -----");
        System.out.println("Getting 3 initial semaphores with a limit of 3...");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNoneEmpty(semaphore.acquireFairSemaphore("testsem", 3, 1000));
        }
        System.out.println("Done!");
        System.out.println("Getting one more that should fail...");
        assert StringUtils.isEmpty(semaphore.acquireFairSemaphore("testsem", 3, 1000));
        System.out.println("Couldn't get it!");
        System.out.println();

        System.out.println("Lets's wait for some of them to time out");
        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = semaphore.acquireFairSemaphore("testsem", 3, 1000);
        assert StringUtils.isNotEmpty(id);
        System.out.println("Got one!");
        System.out.println("Let's release it...");
        assert semaphore.releaseFairSemaphore("testsem", id);
        System.out.println("Released!");
        System.out.println();
        System.out.println("And let's make sure we can get 3 more");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNotEmpty(semaphore.acquireFairSemaphore("testsem", 3, 1000));
        }
        System.out.println("We got them!");
    }

    @Test
    public void testAcquireFairSemaphoreWithLock() throws InterruptedException {
        System.out.println("\n----- testAcquireFairSemaphoreWithLock -----");
        System.out.println("Getting 3 initial semaphores with a limit of 3...");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNoneEmpty(semaphore.acquireFairSemaphoreWithLock("testsem", 3, 1000));
        }
        System.out.println("Done!");
        System.out.println("Getting one more that should fail...");
        assert StringUtils.isEmpty(semaphore.acquireFairSemaphoreWithLock("testsem", 3, 1000));
        System.out.println("Couldn't get it!");
        System.out.println();

        System.out.println("Lets's wait for some of them to time out");
        Thread.sleep(2000);
        System.out.println("Can we get one?");
        String id = semaphore.acquireFairSemaphoreWithLock("testsem", 3, 1000);
        assert StringUtils.isNotEmpty(id);
        System.out.println("Got one!");
        System.out.println("Let's release it...");
        assert semaphore.releaseFairSemaphore("testsem", id);
        System.out.println("Released!");
        System.out.println();
        System.out.println("And let's make sure we can get 3 more");
        for (int i = 0; i < 3; i++) {
            assert StringUtils.isNotEmpty(semaphore.acquireFairSemaphoreWithLock("testsem", 3, 1000));
        }
        System.out.println("We got them!");
    }

    @After
    public void destroy() {
        conn.flushDB();
    }

}