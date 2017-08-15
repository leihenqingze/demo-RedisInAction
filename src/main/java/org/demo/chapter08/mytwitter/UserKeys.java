package org.demo.chapter08.mytwitter;

/**
 * Created by lhqz on 2017/8/15.
 * 用户信息相关key
 */
public class UserKeys {

    //获取自增用户id的key
    public final static String ID_KEY = "user:id";
    //存放用户名的集合
    public final static String LOGIN_KEY = "users:";

    //用户信息散列的key
    public static String userKey(long id) {
        return "user:" + id;
    }

    //锁定用户名的key
    public static String lockKey(String login) {
        return "user:" + login;
    }

    public final static String LOGIN = "login";   //用户登陆名
    public final static String ID = "id";   //用户id
    public final static String NAME = "name";    //用户名称
    public final static String FOLLOWERS = "followers"; //用户拥有的关注者人数
    public final static String FOLLOWING = "following"; //用户正在关注的人数
    public final static String POSTS = "posts";     //用户已发布的状态消息的数量
    public final static String SIGNUP = "signup";    //用户的注册日期

    //个人时间线key前缀
    public final static String HOME_KEY = "home:";

    //主页时间线key前缀
    public final static String PROFILE_KEY = "profile:";

    //时间线key
    public static String timelineKey(String timeline, long id) {
        return timeline + id;
    }

    //正在被我关注的用户集合key
    public static String followingKey(long myId){
        return "following:" + myId;
    }

    //正在关注我的用户集合key
    public static String followersKey(long followId){
        return "followers:" + followId;
    }

}