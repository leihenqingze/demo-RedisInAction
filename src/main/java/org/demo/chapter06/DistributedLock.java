package org.demo.chapter06;

import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

/**
 * Created by lhqz on 2017/8/5.
 * 分布式锁
 * Redis使用WATCH来替代对数据进行加锁,因为WATCH只会在数据被其他客户端
 * 抢先修改了的情况下通知执行了这个命令的客户端,而不会阻止其他客户端堆数
 * 据进行修改,所以这个命令被称为乐观锁.
 * <p>
 * 我们构建的锁不是给同一个进程中的多个线程使用,也不是给同一台机器的多个
 * 进程使用,而是由不同机器上的不同Redis客户端进行获取和释放的.
 * <p>
 * 何时使用以及是否使用WATCH或者锁取决于给定的应用程序:有的应用不需要锁
 * 就可以正确地运行,而有的只需要使用少量的锁,还有些应用需要在每个步骤都
 * 使用锁,不一而足.
 * <p>
 * 我们没有直接使用操作系统级别的锁,编程语言级别的锁,或者其他各式各样的
 * 锁,而是选择了花费大量时间去使用Redis构建锁,这其中一个原因和范围有关;
 * 为了对Redis存储的数据进行排他性的访问,客户端需要访问一个锁,这个锁必
 * 须定义在一个可以让所有客户端都看得见的范围之内,而这个范围就是Redis本
 * 身,因此我们需要把锁构建在Redis里面.另一方面,虽然Redis提供的SETNX命
 * 令确实具有基本的加锁功能,但它的功能并不完整,并且也不具备分布式锁常见
 * 的一些高级特性,所以我们还是需要自己动手来构建分布式锁.
 * <p>
 * Redis使用的是单进程的事件IO驱动模型,也就是说Redis是一个单线程的程序
 * 那为什么还需要锁,这是应为,每个客户端是独立的,不同的客户端之间会存在并
 * 发.既然Redis有了事务,而单个事务中的命是不会分开执行的,那么为什么还需
 * 要锁,这是因为,客户端可能会根据上一次访问的结果来判断下面执行什么操作,
 * 而如果在事务执行时,有其他客户端修改了之前的值,就会造成数据不一致.
 * <p>
 * 之所以在有了WATCH任然需要构建锁的,是因为WATCH命令在频繁修改的时候,冲
 * 突会很多,这时候程序在尝试完成一个事务的时候,可能会因为事务执行失败而反
 * 复地进行重试.
 * <p>
 * 锁在不正确运行时的症状:
 * 1.持有锁的进程因为操作时间过长而导致锁被自动释放,但进程本身并不知道这一
 * 点,甚至还可能会错误地释放掉了其他进程持有的锁.
 * 2.一个持有锁并打算执行长时间操作的进程已经奔溃,但其他想要获取锁的进程不
 * 知道哪个进程持有着锁,也无法检测出持有锁的进程已经奔溃,只能白白地浪费时间
 * 等待锁被释放.
 * 3.在一个进程持有的锁过期之后,其他多个进程同时尝试去获取锁,并且都获得了锁.
 * 4.上面提到的第一种和第三种情况同时出现,导致有多个进程获得了锁,而每个进程
 * 都以为自己是唯一一个获得所的进程.
 * <p>
 * 在高负载情况下,使用锁可以减少重试次数、降低延迟时间、提升性能并将加锁的粒
 * 度调整至合适的大小.
 * <p>
 * dogpile效应:执行事务所需的时间越长,就会有越多待处理的事务相互重叠,这种重
 * 叠增加了执行单个事务所需的时间,并使得那些带有时间限制的事务失败的几率大幅上
 * 升,最终导致所有事务执行失败而的几率和进行重试的几率都大幅地上升,这对于WATCH
 * 来实现的操作来说,影响尤为严重.
 * <p>
 * 在一些情况下,判断应该锁住整个结构还是应该锁住结构中的一小部分是一件非常简
 * 单的事情.比如操作的数据只是一小部分,而只锁住指定的部分无疑是正确的.但是,在
 * 需要锁住的一小部分数据有不止一份的时候,或者需要锁住结构的多个部分的时候,判
 * 断应该对小部分数据进行加锁还是应该直接锁住整个结构就会变得困难起来.除此之外,
 * 使用多个细粒度锁也有引发死锁的风险.
 */
@Data
public class DistributedLock {

    private Jedis conn;

    public String acquireLock(String lockName) {
        return acquireLock(lockName, 10000);
    }

    /**
     * 获取锁
     * 这个锁比较简单,在一些情况下可能会无法正常运行.
     * SETNX命令天生适合用来实现锁的获取功能.这个命令只会在键不存在的情况下
     * 为键设置值,而锁要做的就是将一个随机生成的128位UUID设置为键的值,并使
     * 用这个值来防止锁被其他进程获得.
     * <p>
     * 如果程序在尝试获取锁的时候失败,那么它将不断地重试,直到成功地取的锁或
     * 者超过给定的时限为止.
     *
     * @param lockName    锁名称
     * @param acquireTime 获取锁超时时间(秒)
     * @return 锁标识
     */
    public String acquireLock(String lockName, long acquireTime) {
        //128位随机标识符
        String identifier = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + (acquireTime * 1000);
        while (System.currentTimeMillis() < end) {
            //尝试获取锁
            if (conn.setnx(getLockKey(lockName), identifier) == 1) {
                return identifier;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     * 释放锁
     * 首先使用WATCH命令监视代表锁的键,接着检查键目前的值是否和加锁时设置的
     * 值相同,并在确认值没有变化之后删除该键(这个检查还可以防止程序错误地释
     * 放同一个锁多次)
     *
     * @param lockName   锁名称
     * @param identifier 锁标识
     * @return 是否成功
     */
    public boolean releaseLock(String lockName, String identifier) {
        String lockKey = getLockKey(lockName);
        while (true) {
            conn.watch(lockKey);
            //检查进程是否仍然持有锁
            if (identifier.equals(conn.get(lockKey))) {
                //释放锁
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> results = trans.exec();
                //有其他客户端修改了锁,重试
                if (null == results) {
                    continue;
                }
                return true;
            }
            conn.unwatch();
            break;
        }
        return false;
    }

    /**
     * 带有超时限制特性的锁
     * 防止持有着崩溃的时候不会自动释放锁,加上超时特性.
     *
     * 为了确保锁在客户端已经奔溃(客户端在执行介于SETNX和EXPIRE之间的时候崩溃是最糟糕的)
     * 的情况下仍然能够自动被释放,客户端会在尝试获取锁失败之后,检查锁的超时时间,并为未设置
     * 超时时间的锁设置超时时间.
     * @param lockName    锁名称
     * @param acquireTime 获取锁超时时间(秒)
     * @param lockTimeout 锁超时时间(秒)
     * @return 锁标识
     */
    public String acquireLockWithTimeout(String lockName, long acquireTime, int lockTimeout) {
        String lockKey = getLockKey(lockName);
        //128位随机标识符
        String identifier = UUID.randomUUID().toString();
        //确保传给EXPIRE的都是整数
        int lockExpire = lockTimeout / 1000;

        long end = System.currentTimeMillis() + (acquireTime * 1000);
        while (System.currentTimeMillis() < end) {
            //获取锁并设置过期时间
            if (conn.setnx(lockKey, identifier) == 1) {
                conn.expire(lockKey, lockExpire);
                return identifier;
            }

            //检查过期时间,并在有需要时对其进行更新
            if (conn.ttl(lockKey) == -1) {
                conn.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    private String getLockKey(String lockName) {
        return "lock:" + lockName;
    }

}