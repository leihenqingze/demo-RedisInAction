package org.demo.chapter08;

import org.demo.chapter06.DistributedLock;
import org.demo.chapter08.mytwitter.MyTwitter;
import org.demo.commons.Page;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * Created by lhqz on 2017/8/15.
 */
public class MyTwitterTest {

    private Jedis conn;
    private MyTwitter twitter;
    private DistributedLock lock;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.104");
        conn.auth("123456");
        conn.select(8);
        conn.flushDB();

        lock = new DistributedLock();
        lock.setConn(conn);
        twitter = new MyTwitter();
        twitter.setConn(conn);
        twitter.setLock(lock);
    }

    @Test
    public void testCreateUserAndStatus() {
        System.out.println("\n----- testCreateUserAndStatus -----");

        assert twitter.createUser("TestUser", "Test User") == 1;
        assert twitter.createUser("TestUser", "Test User2") == -1;

        assert twitter.createStatus(1, "This is a new status message") == 1;
        assert "1".equals(conn.hget("user:1", "posts"));
    }

    @Test
    public void testFollowUnfollowUser() {
        System.out.println("\n----- testFollowUnfollowUser -----");
        assert twitter.createUser("TestUser", "Test User") == 1;
        assert twitter.createUser("TestUser2", "Test User2") == 2;

        assert twitter.followUser(1, 2);
        assert conn.zcard("followers:2") == 1;
        assert conn.zcard("followers:1") == 0;
        assert conn.zcard("following:1") == 1;
        assert conn.zcard("following:2") == 0;
        assert "1".equals(conn.hget("user:1", "following"));
        assert "0".equals(conn.hget("user:2", "following"));
        assert "0".equals(conn.hget("user:1", "followers"));
        assert "1".equals(conn.hget("user:2", "followers"));

        assert !twitter.unfollowUser(2, 1);
        assert twitter.unfollowUser(1, 2);
        assert conn.zcard("followers:2") == 0;
        assert conn.zcard("followers:1") == 0;
        assert conn.zcard("following:1") == 0;
        assert conn.zcard("following:2") == 0;
        assert "0".equals(conn.hget("user:1", "following"));
        assert "0".equals(conn.hget("user:2", "following"));
        assert "0".equals(conn.hget("user:1", "followers"));
        assert "0".equals(conn.hget("user:2", "followers"));
    }

    @Test
    public void testSyndicateStatus()
            throws InterruptedException {
        System.out.println("\n----- testSyndicateStatus -----");

        assert twitter.createUser("TestUser", "Test User") == 1;
        assert twitter.createUser("TestUser2", "Test User2") == 2;

        assert twitter.followUser(1, 2);
        assert conn.zcard("followers:2") == 1;
        assert "1".equals(conn.hget("user:1", "following"));
        assert twitter.postStatus(2, "this is some message content") == 1;
        assert twitter.getStatusMessage(1, "home:", new Page(1, 10)).size() == 1;

        for (int i = 3; i < 11; i++) {
            assert twitter.createUser("TestUser" + i, "Test User" + i) == i;
            twitter.followUser(i, 2);
        }

        MyTwitter.POSTS_PER_PASS = 5;

        assert twitter.postStatus(2, "this is some message content") == 2;
        Thread.sleep(100);
        assert twitter.getStatusMessage(9, "home:", new Page(1, 10)).size() == 2;

        assert twitter.unfollowUser(1, 2);
        assert twitter.getStatusMessage(1, "home:", new Page(1, 10)).size() == 0;
    }

    @Test
    public void testRefillTimeline()
            throws InterruptedException {
        System.out.println("\n----- testRefillTimeline -----");

        assert twitter.createUser("TestUser", "Test User") == 1;
        assert twitter.createUser("TestUser2", "Test User2") == 2;
        assert twitter.createUser("TestUser3", "Test User3") == 3;

        assert twitter.followUser(1, 2);
        assert twitter.followUser(1, 3);

        MyTwitter.HOME_TIMELINE_SIZE = 5;

        for (int i = 0; i < 10; i++) {
            assert twitter.postStatus(2, "message") != -1;
            assert twitter.postStatus(3, "message") != -1;
            Thread.sleep(50);
        }

        assert twitter.getStatusMessage( 1,"home:",new Page(1,10)).size() == 5;
        assert twitter.unfollowUser(1, 2);
        assert twitter.getStatusMessage( 1,"home:",new Page(1,10)).size() == 5;

        List<Map<String,String>> messages = twitter.getStatusMessage( 1,"home:",new Page(1,10));
        for (Map<String,String> message : messages) {
            assert "3".equals(message.get("uid"));
        }

        long statusId = Long.valueOf(messages.get(messages.size() - 1).get("id"));
        assert twitter.deleteStatus(3,statusId);
        assert twitter.getStatusMessage( 1,"home:",new Page(1,10)).size() == 4;
        assert conn.zcard("home:1") == 4;

    }

}