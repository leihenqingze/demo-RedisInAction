package org.demo.chapter06;

import com.google.common.collect.Sets;
import org.demo.chapter06.messagequeue.ChatMessages;
import org.demo.chapter06.messagequeue.MultipleReceiversMQ;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/9.
 */
public class MultipleReceiversMQTest {

    private Jedis conn;
    private MultipleReceiversMQ mq;
    private DistributedLock lock;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.106");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        lock = new DistributedLock();
        lock.setConn(conn);
        mq = new MultipleReceiversMQ();
        mq.setConn(conn);
        mq.setLock(lock);
    }

    @Test
    public void testMultiRecipientMessaging() {
        System.out.println("\n----- testMultiRecipientMessaging -----");
        System.out.println("Let's create a new chat session with some recipients...");
        Set<String> recipients = Sets.newHashSet();
        recipients.add("jeff");
        recipients.add("jenny");
        String chatId = mq.createChat("joe",recipients,"message 1");
        System.out.println("Now let's send a few messages...");
        for (int i = 2; i < 5; i++) {
            mq.sendMessage(chatId,"joe","messages " + i);
        }
        assert conn.zcard("msgs:" + chatId) == 4;
        assert conn.zcard("seen:" + "jeff") == 1;
        System.out.println();

        System.out.println("And let's get the messages that are waiting for jeff and jenny...");
        List<ChatMessages> r1 = mq.fetchPendingMessages("jeff");
        List<ChatMessages> r2 = mq.fetchPendingMessages("jenny");
        System.out.println("They are the same? " + r1.equals(r2));
        assert r1.equals(r2);
        System.out.println("Those messages are:");
        for (ChatMessages chat : r1) {
            System.out.println("\t chatId:" + chat.getChatId());
            System.out.println("\t\t" + chat.getMessages());
            for (Map<String,Object> message : chat.getMessages()) {
                System.out.println("\t\t\t" + message);
            }
        }
        assert conn.zrangeWithScores("seen:" + "jeff",0,0).iterator().next().getScore() == 4;
        System.out.println();

        System.out.println("zs join chat...");
        mq.joinChat("zs",chatId);
        assert conn.zrangeWithScores("seen:" + "zs",0,0).iterator().next().getScore() == 4;
        System.out.println();

        System.out.println("jeff leave chat...");
        mq.leaveChat("jeff",chatId);
        assert conn.zcard("chat:" + chatId) == 3;
    }

}
