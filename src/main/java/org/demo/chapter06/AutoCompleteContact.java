package org.demo.chapter06;

import com.google.common.collect.Lists;
import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;

/**
 * Created by lhqz on 2017/8/5.
 * 自动补全-最近联系人
 * 背景
 * 1.每天几百万玩家在线
 * 2.每个用户最近联系过的100个玩家
 * 分析
 * 因为数百万用户都需要有一个属于自己的联系人列表来存储最近联系过的100个人,
 * 所以我们需要在能够快速向列表里面添加用户或者删除用户的前提下,尽量减少
 * 存储这些联系人列表带来的内存消耗.
 * 最近:时间有序,快速添加、删除
 * 设计
 * 因为Redis列表会以有序的方式来存储元素,并且和Redis提供的其他结构相比,列表占用的内存是最少的.
 * 但是Redis不能对列表中的元素进行筛选,所以元素匹配的工作由客户端来解决.
 * 这种交由客户端来处理的方式,可以尽量减少Redis存储和更新数据所需的内存数量.
 * <p>
 * 虽然删除一个元素的时间与列表长度成正比,但是列表的长度比较短,
 * 所以可以运行的非常好,但是不适合用来处理非常大的列表.
 */
@Data
public class AutoCompleteContact {

    private Jedis conn;

    /**
     * 添加最近联系人
     *
     * @param user    当前用户
     * @param contact 最近联系人
     */
    public void addUpdateContact(String user, String contact) {
        String acList = getContactKey(user);
        //准备执行原子操作
        Transaction trans = conn.multi();
        //如果联系人已经存在,那么移除他
        trans.lrem(acList, 0, contact);
        //将联系人推入列表的最前端
        trans.lpush(acList, contact);
        //只保留列表里面的前100个联系人
        trans.ltrim(acList, 0, 99);
        //实际执行以上操作
        trans.exec();
    }

    /**
     * 移除指定最近联系人
     *
     * @param user    当前用户
     * @param contact 最近联系人
     */
    public void removeContact(String user, String contact) {
        String acList = getContactKey(user);
        conn.lrem(acList, 0, contact);
    }

    /**
     * 获取自动补全
     *
     * @param user   当前用户
     * @param prefix 前缀
     * @return 联系人列表
     */
    public List<String> fetchAutoCompleteList(String user, String prefix) {
        String acList = getContactKey(user);
        //获取整个自动补全列表
        List<String> candidates = conn.lrange(acList, 0, -1);
        List<String> matches = Lists.newArrayList();
        //检查每个候选联系人
        if (null != candidates && candidates.size() > 0) {
            for (String candidate : candidates) {
                //发现一个匹配的联系人
                if (candidate.startsWith(prefix)) {
                    matches.add(candidate);
                }
            }
        }
        //返回所有匹配的联系人
        return matches;
    }

    public String getContactKey(String user) {
        return "recent:" + user;
    }

}