package org.demo.chapter08.apistream;

import com.google.common.collect.Lists;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * Created by lhqz on 2017/8/14.
 * 流API(事件处理)——与普通请求需要尽快地执行并完成,
 * 一次性地将全部回复返回给请求不同,
 * 流API请求需要在一段比较长的时间内持续(增量)地返回数据.
 * 1.程序自己处理所有事件(同步、高耦合)
 * 2.回调(同步或异步、耦合较低)
 * 3.MQ(异步、降低耦合、可以被外部系统、第三方合作伙伴处理)
 * <p>
 * 流API的问题
 * 1.流API需要对外公开哪些事件?
 * 2.是否需要进行访问限制?如果需要的话,采取何种方式实现?
 * 3.流API应该提供哪些过滤选项?
 *
 * 提供数据的方式(消息推送)
 * websocket、SPDY、http长连接
 * <p>
 * Jedis使用Redis的pub/sub模式
 * 1.定义Subscriber继承JedisPubSub类
 * 2.由于jedis.subscribe方法会一直阻塞,所以放在单独的线程中
 * 3.在应用中调用jedis.publish发布对应的事件
 *
 * 实现流API的目的是为了演示Redis的pub/sub模式,但是开发流API示例需要的东西很多,
 * 为了突出演示Redis的pub/sub模式,所以这里没有开发流API示例,只是开发了一个pub/sub
 * 的demo.然后对流API的实现进行描述.
 * 1.在应用中增加事件发送代码
 * 2.编写Subscriber监听应用的事件
 * 3.服务端等待客户端请求
 * 4.客户端发送请求
 * 6.服务端接收请求
 * 7.服务端对请求进行解析,通过一个Map<OutputStream,List<Filter>>对客户端进行标识
 * 8.服务端对请求参数进行解析,创建客户端对应的过滤器对象列表,加入到客户端标识Map中
 * 9.当Subscriber监听到消息时,对客户端流进行循环.
 * 10.通过客户端设置的过滤条件对消息进行过滤.
 * 11.把过滤之后的数据发送给客户端.
 * 12.在发送过程中出现异常,则断开客户端.
 *
 * 上面描述的是公共流,如果允许客户端需要预先设置过滤器,而不是在每次请求的时候进行处理.
 * 则还需要对用户进行登录验证,使用客户端的登录名进行标识请求.
 * 上面描述的过程,不会对事件进行记录,如果客户端在断开的期间有新的事件产生,则客户端端
 * 会错失这些事件,如果防止这种情况,这需要在服务端记录不同客户端和未接收的事件进行存储,
 * 或者可以使用第6章开发的消息队列进行处理.
 * 这里虽然是演示,但是加上对网络请求的处理,就可以实现一个完整的流API了.
 */
public class ApiStream {

    public final static String channel = "mychannel";

    public static void main(String[] args) {
        String redisIp = "192.168.0.100";
        int reidsPort = 6379;
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, reidsPort,
                10000, "123456", 8);
        Subscriber subscriber = new Subscriber();
        List<Filter> filters = Lists.newArrayList();
        filters.add(new SimpleFilter());
        subscriber.add(System.out,filters);
        SubThread subThread = new SubThread(jedisPool, subscriber);
        subThread.start();

        Publisher publisher = new Publisher(jedisPool);
        publisher.start();
    }

}