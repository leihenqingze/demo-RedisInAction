package org.demo.chapter06.delaytaskqueue;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.UUID;

/**
 * Created by lhqz on 2017/8/8.
 * 延迟任务队列
 * <p>
 * 实现方式
 * 1.在任务信息中包含任务的执行时间,如果工作队列进程发现任务
 * 的执行时间尚未来临,那么它将暂停等待之后,把任务重新推入队列里面.
 * 短暂的等待和将任务队列推入队列里面都会浪费工作线程的时间.
 * <p>
 * 2.工作进程使用一个本地的等待队列来记录所有需要在未来执行的任务,
 * 并在每次进行while循环的时候,检查等待队列并执行那些已经到期的任务.
 * 工作线程可能会因为崩溃而丢失本地记录的所有待执行任务.
 * <p>
 * 3.把任务添加到一个有序集合,分值为延迟的时间,使用一个线程来检查集合
 * 中是否有立即执行的任务,有的话,就从有序集合中移除,并添加到适当的任务
 * 队列中.
 * <p>
 * 优先级:
 * 可以像任普通任务队列一样通过多个list实现优先级,
 * 或者使同级别的延迟任务优先与普通任务执行.
 * 如:high-delayed,high,medium-delayed,medium,low-delayed,low
 */
@Data
public class DelayTaskQueue {

    private Jedis conn;

    /**
     * 添加延迟任务
     *
     * @param queue 队列名称
     * @param name  回调名称
     * @param args  回调参数
     * @param delay 延迟时间
     * @return 任务标识
     */
    public String executeLater(String queue, String name, List<String> args, long delay) {
        //生成唯一标识符
        String identifier = UUID.randomUUID().toString();
        String itemArgs = JSON.toJSONString(args);
        //准备好要入队的任务
        String item = JSON.toJSONString(new String[]{identifier, queue, name, itemArgs});
        if (delay > 0) {
            conn.zadd(Keys.DELAYED, System.currentTimeMillis() + delay, item);
        } else {
            //立即执行这个任务
            conn.rpush(Keys.getQueueKey(queue), item);
        }
        //返回标识符
        return identifier;
    }

}
