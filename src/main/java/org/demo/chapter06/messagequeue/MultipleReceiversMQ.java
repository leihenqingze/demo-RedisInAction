package org.demo.chapter06.messagequeue;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.demo.chapter06.DistributedLock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/8.
 * 多个接收者的消息队列
 *
 * 单个接收者的消息队列与任务队列相同
 *
 * 消息队列获取消息的两种方式pull和push
 *
 * 这里使用的有序集合而不是列表,是因为使用有序集合可以方便地从每个群组里面取出当前的消息ID
 * 通过将加锁操作交给消息发送者来执行,消息接收者可以免于请求额外的数据,
 * 并且也无需在执行清理操作时进行加锁,这从总体上提高了性能.
 */
@Data
public class MultipleReceiversMQ {

    private Jedis conn;
    private DistributedLock lock;

    private final static String CHAT_ID = "ids:chat:";

    private static String getChatKey(String chatId) {
        return "chat:" + chatId;
    }

    private static String getSeenKey(String recipient) {
        return "seen:" + recipient;
    }

    private static String getMsgsKey(String chatId) {
        return "msgs:" + chatId;
    }

    private static String getMsgIdKey(String chatId) {
        return "ids:" + chatId;
    }

    public String createChat(String sender, Set<String> recipients, String message) {
        String chatId = String.valueOf(conn.incr(CHAT_ID));
        return createChat(sender, recipients, message, chatId);
    }

    /**
     * 创建群组
     *
     * @param sender     发送者(群组创建者)
     * @param recipients 初始用户
     * @param message    初始消息内容
     * @param chatId     群组ID
     * @return 群组ID
     */
    public String createChat(String sender, Set<String> recipients, String message, String chatId) {
        //将当前用户添加到初始用户中
        recipients.add(sender);
        Transaction trans = conn.multi();
        for (String recipient : recipients) {
            //将初始用户全部添加到一个有序集合中,
            //并将这些用户在群组里面的最大已读消息ID初始化为0
            trans.zadd(getChatKey(chatId), 0, recipient);
            //将新群组ID添加到记录用户已参加群组的有序集合里面
            trans.zadd(getSeenKey(recipient), 0, chatId);
        }
        trans.exec();
        //发送初始化消息
        return sendMessage(chatId, sender, message);
    }

    /**
     * 发送消息
     *
     * @param chatId  群组ID
     * @param sender  发送者
     * @param message 消息内容
     * @return 群组ID
     */
    public String sendMessage(String chatId, String sender, String message) {
        //尝试获取锁
        String identifier = lock.acquireLockWithTimeout(getChatKey(chatId), 1000, 1000);
        if (StringUtils.isEmpty(identifier)) {
            throw new RuntimeException("Couldn't get the lock");
        }
        try {
            //获取消息ID
            long messageId = conn.incr(getMsgIdKey(chatId));
            //准备待发送消息
            Map<String, Object> values = Maps.newHashMap();
            values.put("id", messageId);
            values.put("ts", System.currentTimeMillis());
            values.put("sender", sender);
            values.put("message", message);

            //将消息发送至群组
            String json = JSON.toJSONString(values);
            conn.zadd(getMsgsKey(chatId), messageId, json);
        } finally {
            lock.releaseLock(getChatKey(chatId), identifier);
        }
        return chatId;
    }

    /**
     * 获取消息
     *
     * @param recipient 用户
     * @return 消息集合
     */
    public List<ChatMessages> fetchPendingMessages(String recipient) {
        //获取最后接收消息的ID
        Set<Tuple> seenSet = conn.zrangeWithScores(getSeenKey(recipient), 0, -1);
        //使集合有序
        List<Tuple> seenList = Lists.newArrayList(seenSet);
        //获取所有未读消息
        Transaction trans = conn.multi();
        for (Tuple tuple : seenList) {
            String chatId = tuple.getElement();
            int seenId = (int) tuple.getScore();
            trans.zrangeByScore(getMsgsKey(chatId), String.valueOf(seenId + 1), "inf");
        }
        List<Object> results = trans.exec();

        Iterator<Tuple> seenIterator = seenList.iterator();
        Iterator<Object> resultsIterator = results.iterator();

        //消息集合
        List<ChatMessages> chatMessages = Lists.newArrayList();
        List<Object[]> seenUpdates = Lists.newArrayList();
        List<Object[]> msgRemoves = Lists.newArrayList();
        while (seenIterator.hasNext()) {
            //获取群组
            Tuple seen = seenIterator.next();
            //获取群组对应的消息
            Set<String> messageStrings = (Set<String>) resultsIterator.next();
            //如果消息为空则继续
            if (null == messageStrings || messageStrings.size() == 0) {
                continue;
            }

            //当前查看的消息的ID
            int seenId = 0;
            String chatId = seen.getElement();
            List<Map<String, Object>> messages = Lists.newArrayList();
            //解析消息
            for (String messageJson : messageStrings) {
                Map<String, Object> message = (Map<String, Object>) JSON.parse(messageJson);
                Integer messageId = ((Integer) message.get("id"));
                if (messageId > seenId) {
                    seenId = messageId;
                }
                message.put("id", messageId);
                messages.add(message);
            }

            //更新群组成员的消息查看ID
            conn.zadd(getChatKey(chatId), seenId, recipient);
            //添加已查看集合的消息和群组
            seenUpdates.add(new Object[]{getSeenKey(recipient), seenId, chatId});

            //获取群主成员中查看消息最小的ID
            Set<Tuple> minIdSet = conn.zrangeWithScores(getChatKey(chatId), 0, 0);
            if (minIdSet.size() > 0) {
                msgRemoves.add(new Object[]{getMsgsKey(chatId), minIdSet.iterator().next().getScore()});
            }
            //将消息加入结果集合
            chatMessages.add(new ChatMessages(chatId, messages));
        }

        trans = conn.multi();
        //更新已读消息集合
        for (Object[] seenUpdate : seenUpdates) {
            trans.zadd((String) seenUpdate[0], (Integer) seenUpdate[1], (String) seenUpdate[2]);
        }
        //删除已经被所有人查看过的消息
        for (Object[] msgRemove : msgRemoves) {
            trans.zremrangeByScore((String) msgRemove[0], 0, ((Double) msgRemove[1]).intValue());
        }
        trans.exec();
        return chatMessages;
    }

    /**
     * 加入群组
     *
     * @param recipient 用户
     * @param chatId    群组ID
     */
    public void joinChat(String recipient, String chatId) {
        //取得最新群组消息ID
        Integer messageId = Integer.valueOf(conn.get(getMsgIdKey(chatId)));
        Transaction trans = conn.multi();
        //将用户添加到群组成员列表里面
        trans.zadd(getChatKey(chatId), messageId, recipient);
        //将群组添加到用户的已读列表里面
        trans.zadd(getSeenKey(recipient), messageId, chatId);
        trans.exec();
    }

    /**
     * 离开群组
     *
     * @param recipient 用户
     * @param chatId    群组ID
     */
    public void leaveChat(String recipient, String chatId) {
        Transaction trans = conn.multi();
        //从群组里面移除给定的用户
        trans.zrem(getChatKey(chatId), recipient);
        trans.zrem(getSeenKey(recipient), chatId);
        //查找群组剩余成员的数量
        trans.zcard(getChatKey(chatId));
        List<Object> results = trans.exec();
        Long count = (Long) results.get(results.size() - 1);

        if (count > 0) {
            //删除群组
            trans = conn.multi();
            trans.del(getMsgsKey(chatId));
            trans.del(getMsgIdKey(chatId));
            trans.exec();
        } else {
            //找出那些已经被所有成员阅读过的消息
            Set<Tuple> minIdSet = conn.zrangeWithScores(getChatKey(chatId), 0, 0);
            //删除那些已经被所有成员阅读过的消息
            conn.zremrangeByScore(getMsgsKey(chatId), 0, minIdSet.iterator().next().getScore());
        }

    }

}