package org.demo.chapter06.delaytaskqueue;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.demo.chapter06.DistributedLock;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/8.
 * 延迟任务获取线程
 * <p>
 * 这里通过将可执行的任务添加到任务队列里面而不是移除它们,
 * 我们可以把获取可执行任务的线程数量限制在一两个之内,
 * 而不必根据工作线程的数量来决定运行多少个获取线程,
 * 这就减少了获取可执行任务所需的花销.
 * <p>
 * 因为有序集合并不具备像列表一样的阻塞弹出机制,所以程序需要不断地进行循环,
 * 并尝试从队列里面获取要被执行的任务,虽然这一操作会增大网络和处理器的负载,
 * 但因为只会执行一两个这样的程序,所以并不会消耗太多资源.如果想进一步减少开销,
 * 可以添加一个自适应方法,让函数在一段时间内都没有发现可执行的任务时,自动延长休眠
 * 时间,或者根据下一个任务的执行时间来决定休眠时间,并将休眠时间的最大值限制为
 * 100ms,从而确保执行时间距离当前时间不远的任务可以及时被执行.
 */
public class PollQueueThread extends Thread {

    private Jedis conn;
    private boolean quit;
    private DistributedLock lock;

    public PollQueueThread(Jedis conn,DistributedLock lock) {
        this.conn = conn;
        this.lock = lock;
    }

    public void quit() {
        this.quit = true;
    }

    @Override
    public void run() {
        while (!quit) {
            //因为所有被延迟的任务存储在同一个有序集合里面,
            //所以程序只需要获取有序集合里面排名第一的元素以及分值就可以了
            //获取队列中的第一个任务
            Set<Tuple> items = conn.zrangeWithScores(Keys.DELAYED, 0, 0);
            //队列没有包含任何任务,或者任务的执行时间味道
            Tuple item = items.size() > 0 ? items.iterator().next() : null;
            if (null == item || item.getScore() > System.currentTimeMillis()) {
                try {
                    sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            //解码要被执行的任务,弄清楚它应该被推入那个任务队列里面
            String json = item.getElement();
            List<String> values = (List<String>) JSON.parse(json);
            String identifier = values.get(0);
            String queue = values.get(1);

            //为了对任务进行移动,尝试获取锁
            String locked = lock.acquireLockWithTimeout(identifier, 1000, 1000);
            //获取锁失败,跳过后续步骤并重试
            if (StringUtils.isEmpty(locked)) {
                continue;
            }
            //从有序集合中删除元素
            if (conn.zrem(Keys.DELAYED, json) == 1) {
                //将任务推入适当的任务队列里面
                conn.rpush(Keys.getQueueKey(queue), json);
            }
            //释放锁
            lock.releaseLock(identifier, locked);
        }
    }
}