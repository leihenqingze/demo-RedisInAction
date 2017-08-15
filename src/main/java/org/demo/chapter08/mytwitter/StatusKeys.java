package org.demo.chapter08.mytwitter;

/**
 * Created by lhqz on 2017/8/15.
 * 状态消息相关key
 */
public class StatusKeys {

    //获取自增状态消息id的key
    public final static String STATUS_ID = "status:id:";

    //状态消息散列的key
    public static String getStatusKey(long id){
        return "status:" + id;
    }

    public final static String MESSAGE = "message"; //状态消息
    public final static String POSTED = "posted";    //发布时间
    public final static String ID = "id";   //状态信息id
    public final static String UID = "uid";     //用户id
    //不必为了获取发布者的用户名而查找发布者的用户对象了,
    //用户名是不变的,不怕数据冗余带来的数据不一致
    public final static String LOGIN = "login";   //用户登陆名

}