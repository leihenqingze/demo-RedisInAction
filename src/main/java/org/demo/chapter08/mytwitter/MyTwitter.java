package org.demo.chapter08.mytwitter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.demo.chapter06.DistributedLock;
import org.demo.commons.Page;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by lhqz on 2017/8/15.
 * 社交网站
 * 需求:
 * 1.创建用户
 * 2.创建状态消息
 * 3.主页时间线
 * 4.个人时间线
 * 5.关注
 * 6.取消关注
 * 7.正在关注列表
 * 8.关注者列表
 * 9.发布状态消息
 * 10.删除已发布状态消息
 * <p>
 * 10.用户列表,对正在关注的用户进行分组
 * 11.创建用户列表
 * 12.获取用户列表
 * 13.更新用户列表
 * <p>
 * 14.私人用户,关注这些用户需要进过主人的批准
 * 15.收藏(注意状态信息的私密性)
 * 16.用户之间可以进行私聊
 * 17.对消息进行回复将产生一个会话流
 * 18.转发消息
 * 19.使用@指名一个用,或者使用#标记一个话题
 * 20.记录用户使用@指向了谁
 * 21.针对广告行为和滥用行为的投诉与管理机制
 * 22.对状态消息进行"赞"
 * 23.根据"重要性"对消息进行排序
 * 24.在预先设置的一群用户之间进行私聊
 * 25.对用户进行分组,只有组员能够关注组时间线并在里面发布状态消息,小组可以是公开的,私密的甚至是公告形式的
 * 设计:
 * 1.用户:散列,key=user:用户id
 * 2.用户名ID映射:散列,key=users,hkey=用户名,value=用户id(用来确认用户名是否被占用)
 * <p>
 * 重点
 * 尽可能地减少用户在查看页面时系统所需要做的工作
 */
public class MyTwitter {

    public static int HOME_TIMELINE_SIZE = 1000;
    public static int POSTS_PER_PASS = 1000;
    public static int REFILL_USERS_STEP = 50;

    @Setter
    private Jedis conn;
    @Setter
    private DistributedLock lock;

    /**
     * 创建用户
     * 1.检查用户名是否被使用
     * 2.没被使用,获取用户id
     * 3.将用户添加到Redis中
     * 4.将用户名和id添加到用户名id散列中
     *
     * @param login 用户登陆名
     * @param name  用户名称
     * @return 用户id
     */
    public long createUser(String login, String name) {

        String llogin = login.toLowerCase();
        //尝试对小写的登陆名进行加锁
        //防止多个请求在同一时间内使用相同的用户名来创建用户
        String lock = this.lock.acquireLockWithTimeout(UserKeys.lockKey(llogin), 10, 1);
        //如果加锁不成功,那么说明给定的用户名已经被其他用户占用了
        if (StringUtils.isEmpty(lock)) {
            return -1;
        }

        try {
            //用散列来存储小写的用户名以及用户id之间的映射,
            //如果给定的用户名已经被映射到了某个用户id,
            //那么就不会再将这个用户名分配给其他人
            if (StringUtils.isNotEmpty(conn.hget(UserKeys.LOGIN_KEY, llogin))) {
                return -1;
            }

            //通过自增操作生成用户id
            long id = conn.incr(UserKeys.ID_KEY);
            Transaction trans = conn.multi();
            //在散列里面将小写的用户名映射至用户id
            trans.hset(UserKeys.LOGIN_KEY, llogin, String.valueOf(id));
            //将用户信息添加到用户对应的散列里面
            Map<String, String> user = Maps.newHashMap();
            user.put(UserKeys.LOGIN, login);
            user.put(UserKeys.ID, String.valueOf(id));
            user.put(UserKeys.NAME, name);
            user.put(UserKeys.FOLLOWERS, "0");
            user.put(UserKeys.FOLLOWING, "0");
            user.put(UserKeys.POSTS, "0");
            user.put(UserKeys.SIGNUP, String.valueOf(System.currentTimeMillis()));
            trans.hmset(UserKeys.userKey(id), user);
            trans.exec();
            //返回用户id
            return id;
        } finally {
            //释放之前对用户名加的锁
            this.lock.releaseLock(UserKeys.lockKey(llogin), lock);
        }

    }

    /**
     * 创建状态消息
     *
     * @param uid     用户id
     * @param message 状态消息
     * @return 状态消息id
     */
    public long createStatus(long uid, String message) {

        Transaction trans = conn.multi();
        //根据用户id获取用户的用户名
        trans.hget(UserKeys.userKey(uid), UserKeys.LOGIN);
        //为状态消息创建一个新的id
        trans.incr(StatusKeys.STATUS_ID);

        List<Object> response = trans.exec();
        String login = (String) response.get(0);
        long id = (Long) response.get(1);

        //在发布状态消息之前,先验证用户的帐号是否存在
        if (StringUtils.isEmpty(login)) {
            return -1;
        }

        //筹备并设置状态消息的各项信息
        Map<String, String> status = Maps.newHashMap();
        status.put(StatusKeys.MESSAGE, message);
        status.put(StatusKeys.POSTED, String.valueOf(System.currentTimeMillis()));
        status.put(StatusKeys.ID, String.valueOf(id));
        status.put(StatusKeys.UID, String.valueOf(uid));
        status.put(StatusKeys.LOGIN, login);

        trans = conn.multi();
        //保存状态消息
        trans.hmset(StatusKeys.getStatusKey(id), status);
        //更新用户的已发送状态消息数量
        trans.hincrBy(UserKeys.userKey(uid), UserKeys.POSTS, 1);
        trans.exec();
        //返回新创建的状态消息id
        return id;
    }

    /**
     * 主页时间线
     * 因为主页时间线是用户访问网站时的主要入口,所以这些数据必须尽可能地易于获取
     * 主页时间线将成双成对的状态消息id和时间戳记录到了有序集合里面,
     * 其中时间戳用于对状态消息进行排序,而状态消息id则用于获取状态消息本身.
     * <p>
     * 时间线类型:用户id----------zset
     * 状态消息id   |   状态消息发布时间
     *
     * @param uid      用户id
     * @param timeline 时间线类型
     * @param page     分页对象
     * @return 状态信息列表
     */
    public List<Map<String, String>> getStatusMessage(long uid, String timeline, Page page) {
        //获取时间线上面最新的状态消息的id
        Set<String> statusIds = conn.zrevrange(UserKeys.timelineKey(timeline, uid),
                page.getOffset(), page.getLimit());
        List<Map<String, String>> statuses = Lists.newArrayList();
        if (null != statusIds && statusIds.size() > 0) {
            Transaction trans = conn.multi();
            //获取状态消息本身
            for (String id : statusIds) {
                trans.hgetAll(StatusKeys.getStatusKey(Long.valueOf(id)));
            }
            List<Object> data = trans.exec();
            for (Object result : data) {
                Map<String, String> status = (Map<String, String>) result;
                if (null != status && status.size() > 0) {
                    statuses.add(status);
                }
            }
        }
        return statuses;
    }

    /**
     * 关注
     *
     * @param uid      关注者id
     * @param otherUid 被关注者id
     */
    public boolean followUser(long uid, long otherUid) {

        String fkey1 = UserKeys.followingKey(uid);
        String fkey2 = UserKeys.followersKey(otherUid);

        //如果uid指定的用户已经关注了otherUid指定的用户,那么返回
        if (null != conn.zscore(fkey1, String.valueOf(otherUid))) {
            return false;
        }

        long now = System.currentTimeMillis();

        Transaction trans = conn.multi();
        //将两个用户的id分别添加到相应的正在关注有序集合以及
        //关注者有序集合里面
        trans.zadd(fkey1, now, String.valueOf(otherUid));
        trans.zadd(fkey2, now, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        //从被关注用户的个人时间线里面获取HOME_TIMELINE_SIZE条最新的状态消息
        //从而使得用户在关注另一个用户之后,可以立即看见被关注用户所发布的状态消息
        trans.zrevrangeWithScores(UserKeys.timelineKey(UserKeys.PROFILE_KEY, otherUid), 0, HOME_TIMELINE_SIZE - 1);
        List<Object> response = trans.exec();

        long follewing = (Long) response.get(response.size() - 3);
        long follewers = (Long) response.get(response.size() - 2);
        Set<Tuple> statuses = (Set<Tuple>) response.get(response.size() - 1);

        trans = conn.multi();
        //修改两个用户的散列,更新他们各自的正在关注数量以及关注者数量
        trans.hset(UserKeys.userKey(uid), UserKeys.FOLLOWING, String.valueOf(follewing));
        trans.hset(UserKeys.userKey(otherUid), UserKeys.FOLLOWERS, String.valueOf(follewers));
        //对执行关注操作的用户的主页时间线进行更新,
        //并保留时间线上面的最新HOME_TIMELINE_SIZE条状态消息
        if (null != statuses && statuses.size() > 0) {
            for (Tuple status : statuses) {
                trans.zadd(UserKeys.timelineKey(UserKeys.HOME_KEY, uid), status.getScore(), status.getElement());
            }
        }
        trans.zremrangeByRank(UserKeys.timelineKey(UserKeys.HOME_KEY, uid), 0, 0 - HOME_TIMELINE_SIZE - 1);
        trans.exec();
        //返回true表示关注操作已经成功执行
        return true;
    }

    /**
     * 取消关注
     *
     * @param uid      关注者id
     * @param otherUid 被关注者id
     */
    public boolean unfollowUser(long uid, long otherUid) {

        String fkey1 = UserKeys.followingKey(uid);
        String fkey2 = UserKeys.followersKey(otherUid);

        //如果uid指定的用户并未关注otherUid指定的用户,那么返回
        if (null == conn.zscore(fkey1, String.valueOf(otherUid))) {
            return false;
        }

        Transaction trans = conn.multi();
        //从正在关注有序集合以及关注者有序集合里面移除双方的用户的id
        trans.zrem(fkey1, String.valueOf(otherUid));
        trans.zrem(fkey2, String.valueOf(uid));
        trans.zcard(fkey1);
        trans.zcard(fkey2);
        //获取被取消关注用户最近发布的HOME_TIMELINE_SIZE条状态消息
        trans.zrevrange(UserKeys.timelineKey(UserKeys.PROFILE_KEY, otherUid), 0, HOME_TIMELINE_SIZE - 1);
        List<Object> response = trans.exec();

        long follewing = (Long) response.get(response.size() - 3);
        long follewers = (Long) response.get(response.size() - 2);
        Set<String> statuses = (Set<String>) response.get(response.size() - 1);

        trans = conn.multi();
        //修改两个用户的散列,更新他们各自的正在关注数量以及关注者数量
        trans.hset(UserKeys.userKey(uid), UserKeys.FOLLOWING, String.valueOf(follewing));
        trans.hset(UserKeys.userKey(otherUid), UserKeys.FOLLOWERS, String.valueOf(follewers));
        //对执行取消关注操作的用户的主页时间线进行更新,
        //移除被取消关注的用户发布的所有状态消息
        if (null != statuses && statuses.size() > 0) {
            trans.zrem(UserKeys.timelineKey(UserKeys.HOME_KEY, uid), statuses.toArray(new String[statuses.size()]));
        }
        trans.exec();
        refillTimeline(UserKeys.followingKey(uid),
                UserKeys.timelineKey(UserKeys.HOME_KEY, uid), 0);
        //返回true表示取消关注操作已经成功执行
        return true;
    }

    /**
     * 从新填充时间线
     * <p>
     * 从某个用户列表中获取REFILL_USERS_STEP个用户
     * 从每个用户中获取HOME_TIMELINE_SIZE最新的状态消息
     * 循环插入状态消息到用户的某个时间线里面
     * 判断获取的用户数是否大于等于REFILL_USERS_STEP,
     * 如果是则继续调用自己,执行填充,直到用户列表中的最后一个用户
     *
     * @param incoming 某个用户列表
     * @param timeline 某个时间线
     * @param start    上次被更新的最后一个被关注用户在集合中的位置(分值)
     */
    public void refillTimeline(String incoming, String timeline, double start) {
        if (start == 0 && conn.zcard(timeline) >= 750) {
            return;
        }

        //以上次被更新的最后一个被关注用户为起点,获取接下来的50个被关注用户
        Set<Tuple> users = conn.zrangeByScoreWithScores(
                incoming, String.valueOf(start), "inf",
                0, REFILL_USERS_STEP);

        Pipeline pipeline = conn.pipelined();
        for (Tuple tuple : users) {
            String uid = tuple.getElement();
            //在遍历关注者的同时,对start变量进行更新,这个变量可以在
            //有需要的时候传递给下一个refillTimeline调用
            start = tuple.getScore();
            //获取被关注用户的状态消息
            pipeline.zrevrangeWithScores(
                    UserKeys.timelineKey(UserKeys.PROFILE_KEY,
                            Long.valueOf(uid)), 0,
                    HOME_TIMELINE_SIZE - 1);
        }

        List<Object> response = pipeline.syncAndReturnAll();
        List<Tuple> messages = Lists.newArrayList();
        //将多个被关注者的状态消息加入到一个集合中
        for (Object results : response) {
            messages.addAll((Set<Tuple>) results);
        }
        //对状态消息集合排序
        Collections.sort(messages);

        //对状态消息集合进行修剪
        if (messages.size() > HOME_TIMELINE_SIZE){
            messages = messages.subList(0, HOME_TIMELINE_SIZE);
        }

        Transaction trans = conn.multi();
        if (messages.size() > 0) {
            //循环将状态消息更新到用户的主页时间线中
            for (Tuple tuple : messages) {
                trans.zadd(timeline, tuple.getScore(),
                        tuple.getElement());
            }
        }
        //对用户的主页时间线进行修剪
        trans.zremrangeByRank(timeline, 0,
                0 - HOME_TIMELINE_SIZE - 1);
        trans.exec();

        //如果需要更新的被关注用户数量超过50人,那么在执行延迟任务里面继续执行剩余的更新操作
        if (users.size() >= REFILL_USERS_STEP) {
            try {
                Method method = getClass().getDeclaredMethod("refillTimeline", String.class, String.class, Double.TYPE);
                executeLater("default", method, incoming, timeline, start);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 发布状态消息
     *
     * @param uid     用户id
     * @param message 状态消息
     * @return 状态消息id
     */
    public long postStatus(long uid, String message) {
        //创建一条新的状态消息
        long id = createStatus(uid, message);
        //如果创建状态消息失败,那么直接返回
        if (id == -1) {
            return -1;
        }

        //获取消息的发布时间
        String postedString = conn.hget(StatusKeys.getStatusKey(id), StatusKeys.POSTED);
        //如果程序未能顺利地获取状态消息的发布时间,那么直接返回
        if (StringUtils.isEmpty(postedString)) {
            return -1;
        }

        //将状态消息添加到用户的个人时间线里面
        long posted = Long.valueOf(postedString);
        conn.zadd(UserKeys.timelineKey(UserKeys.PROFILE_KEY, uid), posted, String.valueOf(id));
        //将状态消息推送给用户的关注者
        syndicateStatus(uid, id, posted, 0);
        return id;
    }

    /**
     * 同步状态消息到关注用户的时间线中
     *
     * @param uid    用户id
     * @param id     状态消息id
     * @param posted 状态消息发布时间
     * @param start  上次被更新的最后一个关注者在集合中的位置(分值)
     */
    public void syndicateStatus(long uid, long id, long posted, double start) {

        //以上次被更新的最后一个关注者为起点,获取接下来的1000个关注者
        Set<Tuple> followers = conn.zrangeByScoreWithScores(
                UserKeys.followersKey(uid), String.valueOf(start),
                "inf", 0, POSTS_PER_PASS);

        Transaction trans = conn.multi();
        for (Tuple tuple : followers) {
            String follower = tuple.getElement();
            //在遍历关注者的同时,对start变量进行更新,这个变量可以在
            //有需要的时候传递给下一个syndicateStatus调用
            start = tuple.getScore();
            //将状态消息添加到所有被获取的关注者的主页时间线里面,
            //并在有需要的时候对关注者的主页时间线进行修剪,防止它超过限定的最大长度
            trans.zadd(UserKeys.timelineKey(UserKeys.HOME_KEY,
                    Long.valueOf(follower)), posted, String.valueOf(id));
            trans.zremrangeByRank(UserKeys.timelineKey(UserKeys.HOME_KEY,
                    Long.valueOf(follower))
                    , 0, 0 - HOME_TIMELINE_SIZE - 1);
        }
        trans.exec();

        //如果需要更新的关注者数量超过1000人,那么在执行延迟任务里面继续执行剩余的更新操作
        if (followers.size() >= POSTS_PER_PASS) {
            try {
                Method method = getClass().getDeclaredMethod(
                        "syndicateStatus", Long.TYPE, Long.TYPE,
                        Long.TYPE, Double.TYPE);
                executeLater("default", method, uid,
                        id, posted, start);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除状态消息
     *
     * @param uid
     * @param statusId
     * @return
     */
    public boolean deleteStatus(long uid, long statusId) {

        String key = StatusKeys.getStatusKey(statusId);
        //对指定的状态消息加锁,防止两个程序同时删除同一条状态消息的情况出现
        String lock = this.lock.acquireLockWithTimeout(key, 1, 10);
        //如果加锁失败,那么直接返回
        if (StringUtils.isEmpty(lock)) {
            return false;
        }

        try {
            //如果uid指定的用户并非状态消息的发布人,那么直接返回
            if (!Objects.equals(conn.hget(key, StatusKeys.UID), String.valueOf(uid))) {
                return false;
            }

            Transaction trans = conn.multi();
            //删除指定的状态消息
            trans.del(key);
            //从用户的个人时间线里面移除被删除的状态消息id
            trans.zrem(UserKeys.timelineKey(UserKeys.PROFILE_KEY, uid), String.valueOf(statusId));
            //从用户的主页时间线里面移除被删除的状态消息id
            trans.zrem(UserKeys.timelineKey(UserKeys.HOME_KEY, uid), String.valueOf(statusId));
            //对存储着用户信息的散列更新,减少已发布状态消息的数量
            trans.hincrBy(UserKeys.userKey(uid), UserKeys.POSTS, -1);
            trans.exec();
            cleanTimelines(uid, statusId, 0);
            return true;
        } finally {
            this.lock.releaseLock(key, lock);
        }
    }

    /**
     * 从关注用户的时间线中清楚删除的状态消息
     *
     * @param uid   用户id
     * @param id    状态消息id
     * @param start 上次被更新的最后一个关注者在集合中的位置(分值)
     */
    public void cleanTimelines(long uid, long id, double start) {

        //以上次被更新的最后一个关注者为起点,获取接下来的1000个关注者
        Set<Tuple> followers = conn.zrangeByScoreWithScores(
                UserKeys.followersKey(uid), String.valueOf(start),
                "inf", 0, POSTS_PER_PASS);

        Transaction trans = conn.multi();
        for (Tuple tuple : followers) {
            String follower = tuple.getElement();
            //在遍历关注者的同时,对start变量进行更新,这个变量可以在
            //有需要的时候传递给下一个syndicateStatus调用
            start = tuple.getScore();
            //从关注者的主页时间线里面清楚已删除的状态消息
            trans.zrem(UserKeys.timelineKey(UserKeys.HOME_KEY,
                    Long.valueOf(follower)), String.valueOf(id));
        }
        trans.exec();

        //如果需要更新的关注者数量超过1000人,那么在执行延迟任务里面继续执行剩余的清楚操作
        if (followers.size() >= POSTS_PER_PASS) {
            try {
                Method method = getClass().getDeclaredMethod(
                        "cleanTimelines", Long.TYPE, Long.TYPE,
                        Long.TYPE, Double.TYPE);
                executeLater("default", method, uid,
                        id, start);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 延迟执行方法
     *
     * @param queue  延迟队列名
     * @param method 被调用的方法
     * @param args   被调用方法的参数
     */
    public void executeLater(String queue, Method method, Object... args) {
        MethodThread thread = new MethodThread(this, method, args);
        thread.start();
    }

}