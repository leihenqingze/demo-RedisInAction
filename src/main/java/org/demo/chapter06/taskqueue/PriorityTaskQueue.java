package org.demo.chapter06.taskqueue;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.Data;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by lhqz on 2017/8/7.
 */
@Data
public class PriorityTaskQueue {

    private Jedis conn;

    /**
     * 多任务的优先级队列
     *
     * @param taskName 任务类型
     * @param priority 优先级
     */
    public void sendSoldEmailViaQueue(String taskName, Integer priority, String seller, String item, double price, String buger) {
        //准备待发送数据
        Map<String, Object> data = Maps.newHashMap();
        data.put(TaskQueueKeys.TASKNAME, taskName);
        data.put("seller_id", seller);
        data.put("item_id", item);
        data.put("price", price);
        data.put("buger_id", buger);
        data.put("time", System.currentTimeMillis());
        String json = JSON.toJSONString(data);
        //将待处理任务推入队列里面
        conn.rpush(TaskQueueKeys.getQueueName(priority), json);
    }

}
