package org.demo.chapter07;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/12.
 */
public class IndexSearchJobTest {

    private Jedis conn;
    private IndexSearchJob isj;
    private MyES es;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.110");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        isj = new IndexSearchJob();
        isj.setConn(conn);
        es = new MyES();
        es.setConn(conn);
        isj.setEs(es);

    }

    @Test
    public void testIndexAndFindJobs() {
        System.out.println("\n----- testIndexAndFindJobs -----");
        isj.indexJob("test1", "q1", "q2", "q3");
        isj.indexJob("test2", "q1", "q3", "q4");
        isj.indexJob("test3", "q1", "q3", "q5");

        assert isj.findJobs("q1").size() == 0;

        Iterator<String> result = isj.findJobs("q1", "q3", "q4").iterator();
        assert "test2".equals(result.next());

        result = isj.findJobs("q1", "q3", "q5").iterator();
        assert "test3".equals(result.next());

        result = isj.findJobs("q1", "q2", "q3", "q4", "q5").iterator();
        assert "test1".equals(result.next());
        assert "test2".equals(result.next());
        assert "test3".equals(result.next());

        result = isj.findJobs("q1", "q2", "q3", "q6").iterator();
        assert "test1".equals(result.next());

    }

}