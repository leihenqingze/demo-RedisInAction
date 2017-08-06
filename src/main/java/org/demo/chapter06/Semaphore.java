package org.demo.chapter06;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.UUID;

/**
 * Created by lhqz on 2017/8/6.
 * 计数信号量
 * 计数信号量是一种锁,通常用于限定能够同时使用的资源数量.
 * 计数信号量和其他锁的区别在于,当客户端获取锁失败的时候,客户端通常会选择进行等待;
 * 而当客户端获取计数信号量失败的时候,客户端通常会选择立即返回失败结果.
 * <p>
 * 计数信号量的问题
 * 1.判断是哪个客户端取得了锁
 * 2.处理客户端在获得锁之后奔溃的情况
 * 3.锁超时
 * <p>
 * 构建计数信号量的方式
 * 1.使用EXPIRE
 * 2.使用有序集合(将多个信号量持有者的信息存储到同一个结构里面)
 * <p>
 * semaphore:信号量名称-------zset
 * 信号量标识符   | 时间戳
 * <p>
 * 常见应用场景
 * 限制同时可运行的API调用数量
 * 用于限制针对数据库的并发请求数量,从而降低执行单个查询所需的时间
 * 限制下载文件的页面数量
 * <p>
 * 如果对于使用系统时钟没有意见、也不需要对信号量进行刷新,并且能够接受信号量
 * 的数量偶尔超过限制,那么可以使用第一个信号量实现.
 * <p>
 * 如果只信任一两秒之间的系统时钟,但仍然能够接受信号量的数量偶尔超过限制,那么
 * 可以使用第二种信号量实现
 * <p>
 * 如果希望信号量一直都具有正确的行为,那么可以使用带锁的信号量实现来保证正确性
 */
@Data
public class Semaphore {

    private Jedis conn;
    private DistributedLock lock;

    /**
     * 获取信号量
     * 这里依赖于客户端传递时间,假设每个进程访问到的系统时间都是相同的,
     * 但是在多主机环境下可能并不成立.每当锁或者信号量因为系统时钟的细
     * 微不同而导致锁的获取结果出现剧烈变化时,这个锁或者信号量就是不公
     * 平的.不公平的锁或信号量可能会导致客户端永远也无法取得它原本应该
     * 得到的锁或信号量.
     * <p>
     * 系统时间较慢的系统上运行的客户端,将能够偷走系统时钟较快的系统上
     * 运行的客户端已经取得的信号量,导致信号量变的不公平.
     *
     * @param semname 信号量名
     * @param limit   信号量总数
     * @param timeout 过期时间
     * @return 信号量标识符号
     */
    public String acquireSemaphore(String semname, int limit, long timeout) {

        //128位随机标识符
        String identifier = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        Transaction trans = conn.multi();
        //清理过期的信号量
        trans.zremrangeByScore(semname.getBytes(), "-inf".getBytes(), String.valueOf(now - timeout).getBytes());
        //尝试获取信号量
        trans.zadd(semname, now, identifier);
        //检查是否成功取得了信号量
        trans.zrank(semname, identifier);
        List<Object> results = trans.exec();
        //排名低于可获取的信号量总数,表示成功获取信号量
        Long rank = (Long) results.get(results.size() - 1);
        if (rank < limit) {
            return identifier;
        }
        //获取信号量失败,删除之前添加的标识符
        conn.zrem(semname, identifier);
        return null;
    }

    /**
     * 释放信号量
     *
     * @param semname    信号量名
     * @param identifier 信号量标识符
     * @return
     */
    public boolean releaseSemaphore(String semname, String identifier) {
        return conn.zrem(semname, identifier) == 1;
    }

    /**
     * 公平信号量
     * 为了尽可能地减少系统时间不一致带来的问题,我们需要给信号量
     * 实现添加一个计数器以及一个信号量拥有者的有序集合.
     * <p>
     * 信号量拥有者的有序集合
     * semaphore:信号量名称:owner-------zset
     * 信号量标识符   | 计数器
     * <p>
     * 尽管该信号量并不需要所有主机都拥有相同的系统时间,但各个主
     * 机在系统时间上的差距仍然需要控制在一两秒之内,从而避免信号
     * 量过早释放或者太晚释放.
     * <p>
     * 系统时间、添加计数器到拥有者集合、系统的多次往返通信具有竞态条件
     *
     * @param semname 信号量名
     * @param limit   信号量总数
     * @param timeout 过期时间
     * @return 信号量标识符号
     */
    public String acquireFairSemaphore(String semname, int limit, long timeout) {
        //128位随机标识符
        String identifier = UUID.randomUUID().toString();
        String czset = semname + ":owner";
        String ctr = semname + ":counter";

        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        //移除超时信号量
        trans.zremrangeByScore(semname.getBytes(),
                "-inf".getBytes(), String.valueOf(now - timeout)
                        .getBytes());
        //对超时有序集合和信号量拥有者有序集合执行交集计算,
        //并将计算结果保存到信号量拥有者有序集合里面,覆盖有序集合中原有的数据.
        ZParams params = new ZParams();
        params.weightsByDouble(1, 0);
        trans.zinterstore(czset, params, czset, semname);
        //对计数器执行自增操作
        trans.incr(ctr);
        List<Object> results = trans.exec();
        //获取计数器在执行自增操作之后的值.
        int counter = ((Long) results.get(results.size() - 1)).intValue();

        trans = conn.multi();
        //将当前系统时间添加到超时有序集合里面
        trans.zadd(semname, now, identifier);
        //将计数器生成的值添加到信号量拥有者有序集合里面
        trans.zadd(czset, counter, identifier);
        trans.zrank(czset, identifier);
        results = trans.exec();
        //通过牌排名来判断客户端是否取得了信号量
        int result = ((Long) results.get(results.size() - 1)).intValue();
        if (result < limit) {
            //客户端成功取得了信号量
            return identifier;
        }

        //客户端未能取得信号量,清理无用数据
        trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(czset, identifier);
        trans.exec();
        return null;
    }

    /**
     * 释放公平信号量
     *
     * @param semname    信号量名
     * @param identifier 信号量标识符
     * @return 是否释放成功
     */
    public boolean releaseFairSemaphore(String semname, String identifier) {
        Transaction trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(semname + ":owner", identifier);
        List<Object> results = trans.exec();
        return (Long) results.get(results.size() - 1) == 1;
    }

    /**
     * 刷新信号量
     *
     * @param semname    信号量名
     * @param identifier 信号量标识符
     * @return 刷新是否成功
     */
    public boolean refreshFairSemaphore(String semname, String identifier) {
        //只要客户端持有的信号量没有因为过期而被删除,更新客户端持有的信号量
        if (conn.zadd(semname, System.currentTimeMillis(), identifier) == 1) {
            //如果客户端持有的信号量已经因为超时而被删除,那么函数将释放信号量
            releaseFairSemaphore(semname, identifier);
            //客户端仍然持有信号量
            return false;
        }
        //告知调用者,客户端已经失去了信号量
        return true;
    }

    /**
     * 带有锁的计数信号量
     *
     * @param semname 信号量名
     * @param limit   信号量总数
     * @param timeout 过期时间
     * @return 信号量标识符号
     */
    public String acquireFairSemaphoreWithLock(String semname, int limit, long timeout) {
        String identifier = lock.acquireLockWithTimeout(semname, 10, 10000);
        if (StringUtils.isNotEmpty(identifier)) {
            try {
                return acquireFairSemaphore(semname, limit, timeout);
            } finally {
                lock.releaseLock(semname, identifier);
            }
        }
        return null;
    }

}