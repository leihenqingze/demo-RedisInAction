package org.demo.chapter07;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by lhqz on 2017/8/12.
 */
public class SimpleSearchJobTest {

    private Jedis conn;
    private SimpleSearchJob ssj;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        ssj = new SimpleSearchJob();
        ssj.setConn(conn);

    }

    @Test
    public void testIsQualifiedForJob() {
        System.out.println("\n----- testIsQualifiedForJob -----");
        ssj.addJob("test","q1","q2","q3");
        assert ssj.isQualified("test","q1","q2","q3");
        assert !ssj.isQualified("test","q1","q2");
        String job = ssj.findJobs("q1","q2","q3").iterator().next();
        assert "test".equals(job);
        Set<String> jobs = ssj.findJobs("q1","q2");
        assert null == jobs || jobs.size() == 0;
    }

}