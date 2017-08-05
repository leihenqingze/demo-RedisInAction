package org.demo.chapter06;

import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by lhqz on 2017/8/5.
 * 自动补全-通讯录
 * <p>
 * 背景:
 * 工会中的成员自动补全
 * <p>
 * 分析:
 * 对于短列表来说在客户端完成匹配是可行的,但对于长列表来说,
 * 仅仅为了找到几个元素而获取成千上万个元素,是一种非常浪费资源的做法.
 * 但是每个工会只需要一个自动补全列表,所以在实现这个列表的时候可以稍微多
 * 花费一些内存.
 * 设计:
 * 为了在客户端进行自动补全的时候,尽量减少服务器需要传输给客户端的数据量,
 * 将使用有序集合来直接在Redis内部完成自动补全的前缀计算工作.
 *
 * 通过向有序集合添加元素来创建查找范围,并在取得范围内的元素之后移除之前添加的元素,
 * 这种技术可以应用在任何已排序索引上面,并且可以对任意类型的数据进行范围查找.
 */
@Data
public class AutoCompleteAddressBook {

    private Jedis conn;

    //准备一个由以知字符组成的列表
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";

    /**
     * 根据给定的前缀计算出查找范围的起点和终点
     * 如:abc的范围为abb{ ~ abc{
     * 这里只支持字母查找,如果需要其他的还需要处理编码问题,并找到替代`和{的字符
     *
     * @param prefix 前缀
     * @return 起点和终点
     */
    public String[] findPrefixRange(String prefix) {
        //获取前缀最后一个字母在编码中的位置
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        //获取前缀最后一个字母在编码中的前一个字母
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        //拼接起点,去掉前缀的最后一个字母拼接上编码中的前一个字母,加上"{"过滤掉开始元素
        String start = prefix.substring(0, prefix.length() - 1) + suffix + "{";
        //拼接终点
        String end = prefix + "{";
        return new String[]{start, end};
    }

    /**
     * 自动补全
     *
     * @param guiId  公会
     * @param prefix 前缀
     * @return 通信录集合
     */
    public Set<String> autocompleteOnPrefix(String guiId, String prefix) {
        //根据给定的前缀计算出查找范围的起点和终点
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        //防止与其他自动补全操作冲突
        String identifier = UUID.randomUUID().toString();
        start += identifier;
        end += identifier;

        String zsetName = getAddressBookKey(guiId);

        //将范围的起始元素和结束元素添加到有序集合里面
        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true) {
            conn.watch(zsetName);
            //找到两个被插入元素在有序集合中的排名
            int sindex = conn.zrank(zsetName, start).intValue();
            int eindex = conn.zrank(zsetName, end).intValue();
            //将范围限制在10个元素之内
            int erange = Math.min(sindex + 9, eindex - 2);

            Transaction trans = conn.multi();
            //获取范围内的值,然后删除之前插入的起始元素和结束元素
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            trans.zrange(zsetName, sindex, erange);
            List<Object> results = trans.exec();
            //如果自动补全有序集合已经被其他客户端修改过了,那么重试
            if (null != results && results.size() > 0) {
                items = (Set<String>) results.get(results.size() - 1);
                break;
            }
        }

        //如果有其他自动补全操作正在执行,那么从获取到的元素里面移除起始元素和结束元素
        if (null != items && items.size() > 0) {
            for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ) {
                if (iterator.next().indexOf('{') != -1) {
                    iterator.remove();
                }
            }
        }
        return items;
    }

    /**
     * 加入公会
     *
     * @param guild 公会
     * @param user  用户
     */
    public void joinGuild(String guild, String user) {
        conn.zadd(getAddressBookKey(guild), 0, user);
    }

    /**
     * 退出公会
     *
     * @param guild 公会
     * @param user  用户
     */
    public void leaveGuild(String guild, String user) {
        conn.zrem(getAddressBookKey(guild), user);
    }

    public String getAddressBookKey(String guiId) {
        return "members:" + guiId;
    }

}