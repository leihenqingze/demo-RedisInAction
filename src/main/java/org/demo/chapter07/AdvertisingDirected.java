package org.demo.chapter07;

import com.google.common.collect.Maps;
import lombok.Setter;
import org.javatuples.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 需求描述
 * 结构设计描述
 * key
 * 面向对象
 * 未实现部分
 * 事务
 * 锁
 * 笔记
 * Created by lhqz on 2017/8/10.
 * 广告定向
 * 用户浏览页面,Web服务器和用户的Web浏览器向广告服务器发送请求获取广告,
 * 广告服务器通过各项参数,并根据这些信息找出能够通过点击、浏览或动作获取最大经济收益的广告.
 * 参数通常有:浏览者的基本位置信息(IP地址、GPS信息)、操作系统、Web浏览器、
 * 正在浏览页面的内容、浏览者在当前网站上最近浏览的一些页面.
 * <p>
 * 广告预算:每个广告通常会有一个随时间减少的预算.
 * <p>
 * 广告计费方式
 * 1.按点击(CPC)
 * 2.按展示(CPM)
 * 3.按动作(CPA)
 * <p>
 * 页面内容附加值结构
 * idx:单词----------zset
 * 广告id     | eCPM增加的数值
 */
public class AdvertisingDirected {

    @Setter
    private Jedis conn;
    @Setter
    private MyES es;

    /**
     * 让广告的价格保持一致的辅助函数(将CPC、CPA转成CPM)
     * 为了尽可能简化广告价格的计算方式,程序将对所有类型的广告进行转换,
     * 使得它们的价格可以基于每千次展示进行计算,产生一个估算CPM,简称eCPM
     * CPC to CPM: 广告的点击通过率CRT(广告的点击次数 / 广告的展示次数) * 1000 * 价格
     * CPA to CPM: 广告的点击通过率CRT * 用户执行动作的概率(用户执行动作次数 / 广告的点击次数) * 1000 * 价格
     * = 用户执行动作次数 / 广告的展示次数 * 1000 ) * 价格
     * <p>
     * 这里使用点击次数、展示次数和动作执行次数作为参数,而不是直接使用已经计算好的点击通过率,
     * 这使得我们可以这些直接存储在记账系统里面,并且只有在有需要的时候才计算eCPM.
     * <p>
     * 计算CPC的eCPM与CPA的eCPM的方法基本相同,主要区别在于:大多数广告的动作执行
     * 次数都会明显少于点击次数,但每个动作的价格通常比每次点击的价格要高不少.
     *
     * @param type  广告计费类型
     * @param views 展示次数
     * @param avg   点击次数或执行动作次数
     * @param value 广告价格
     * @return 计算之后的价格
     */
    public double toEcpm(Ecpm type, double views, double avg, double value) {
        switch (type) {
            case CPC:
            case CPA:
                return 1000. * value * avg / views;
            case CPM:
                return value;
        }
        return value;
    }

    //为了评估广告的效果,程序会记录广告每1000次展示的平均点击次数或平均动作执行次数
    private Map<Ecpm, Double> AVERAGE_PER_1K = Maps.newHashMap();

    /**
     * 将广告加入索引
     * <p>
     * 这里接受两个定向选项:位置和内容
     * 其中位置选项时必须的,而广告与页面内容之间的任何匹配单词则是可选的,
     * 并且只作为广告的附加值存在.
     *
     * @param id        广告id
     * @param locations 地理位置
     * @param content   浏览内容
     * @param type      广告计算价格
     * @param value     广告价格
     */
    public void indexAd(String id, String[] locations, String content, Ecpm type, double value) {
        //设置流水线,使得程序可以在一次通信往返里面完成整个索引操作
        Transaction trans = conn.multi();
        //为了进行定向操作,把广告ID添加到所有相关的位置集合里面
        //必须的位置定向参数会被存储到集合里面,并且这些位置参数不会提供任何附加值
        for (String location : locations) {
            trans.sadd(idxReqKey(location), id);
        }

        //对广告包含的单词进行索引,使用有序集合是为了存放内容的附加值
        Set<String> words = es.tokenize(content);
        for (String word : words) {
            trans.zadd(es.idxKey(word), 0, id);
        }

        //获取广告的平均点击次数或平均动作执行次数,如果不存在则为1
        double avg = AVERAGE_PER_1K.containsKey(type) ? AVERAGE_PER_1K.get(type) : 1;
        //获取基本的eCPM
        double rvalue = toEcpm(type, 1000, avg, value);

        //记录这个广告的类型
        trans.hset("type:", id, type.name().toLowerCase());
        //记录广告的eCPM添加到一个记录了所有广告的eCPM的有序集合里面
        trans.zadd("idx:ad:value:", rvalue, id);
        //将广告的基本价格添加到一个记录所有广告的基本价格的有序集合里面
        trans.zadd("ad:base_value:", value, id);
        //把能够对广告进行定向的单词全部记录起来
        for (String word : words) {
            trans.sadd("terms:" + id, word);
        }
        trans.exec();
    }

    /**
     * 基于位置执行广告定向操作的辅助函数
     * 通过位置执行集合的并集操作,实现对基于位置对广告进行匹配,方便在匹配完成之后,
     * 程序在不添加任何附加值的情况下,计算被匹配广告的基本eCPM
     *
     * @param trans     事务对象
     * @param locations 位置集合
     * @return 位置匹配集合的id
     */
    public String matchLocation(Transaction trans, String[] locations) {
        String[] required = new String[locations.length];
        //根据所有给定的位置,找出需要执行并集操作的集合键
        for (int i = 0; i < locations.length; i++) {
            required[i] = "req:" + locations[i];
        }
        //找出与指定地区相匹配的广告,并将它们存储到集合里面
        return es.union(trans, 300, required);
    }

    /**
     * 计算包含了内容匹配附加值的广告eCPM
     * <p>
     * 内容匹配附加值 = eCPM的加权平均值,每个单词的权重为单词与
     * 页面内容匹配的次数(Redis有序集合不能作除法,没法计算)
     * >= 几何平均数 (由于匹配单词的数量可能会发生变化,没法计算)
     * <= 算数平均数 (由于匹配单词的数量可能会发生变化,没法计算)
     * ~= (最大值 + 最小值) / 2 (这个在数学上是不严谨的,这里使
     * 用是因为实现起来比较简单,能够给出一个合理的结果(加权平均数
     * 介于最大值和最小值之间))
     *
     * @param trans   事务对象
     * @param matched 定向位置id
     * @param base    基本eCPM id
     * @param content 页面内容
     * @return 包含了内容匹配附加值的广告eCPM
     */
    public Pair<Set<String>, String> finishScoring(Transaction trans, String matched, String base, String content) {

        Map<String, Integer> bonusEcpm = Maps.newHashMap();
        //对内容进行标记化处理,以便与广告进行处理
        Set<String> words = es.tokenize(content);
        for (String word : words) {
            //找出那些即位于定向位置之内,又拥有页面内容其中一个单词的广告
            //这里先执行了交集运算,是因为在使用"先计算并集,后计算交集"的方式计算定向广告的附加值时,
            //计算需要处理相关单词附加值有序集合里面存储的所有广告,包括那些并不符合位置匹配要求的广告
            //在使用"先计算交集,后计算并集"的方式计算定向广告的附加值时,计算只需要处理符合位置匹配
            //要求的广告,这极大地减少了Redis需要处理的数据量
            String wordBouns = es.zintersect(trans, 30, new ZParams().weightsByDouble(0, 1), matched, word);
            bonusEcpm.put(wordBouns, 1);
        }

        if (null != bonusEcpm && bonusEcpm.size() > 0) {
            String[] keys = new String[bonusEcpm.size()];
            int[] weights = new int[bonusEcpm.size()];
            int index = 0;
            for (Map.Entry<String, Integer> bonus : bonusEcpm.entrySet()) {
                keys[index] = bonus.getKey();
                weights[index] = bonus.getValue();
                index++;
            }

            //计算每个广告的最小eCPM附加值和最大eCPM附加值
            ZParams minParams = new ZParams().aggregate(ZParams.Aggregate.MIN);
            String minimum = es.zunion(trans, 30, minParams, keys);
            ZParams maxParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
            String maximum = es.zunion(trans, 30, maxParams, keys);
            //将广告的基本价格、最小eCPM附加值的一半以及最大eCPM附加值的一半这三者相加起来
            String result = es.zunion(trans, 30, new ZParams().weightsByDouble(2, 1, 1), base, minimum, maximum);
            return new Pair<Set<String>, String>(words, result);
        }
        //如果页面内容中没有出现任何可匹配的单词,那么返回广告的基本eCPM
        return new Pair<Set<String>, String>(words, base);
    }

    /**
     * 通过位置和页面内容附加值实现广告定向操作
     * 在匹配用户所在位置的一系列广告里面,找出eCPM最高的那一个广告.
     * 除了基于位置对广告进行匹配之外,程序会记录页面内容与广告内容的匹配度,
     * 以及不同匹配度对广告点击通过率的影响等统计数据.通过使用这些统计数据,
     * 广告中与Web页面想匹配的那些内容就会作为附加值被计入有CPC和CPA计算
     * 出的eCPM里面,使得那些包含了匹配内容的广告能够更多地被展示出来.
     * <p>
     * 在展示广告之前,系统不会为Web页面的任何内容设置附加值.但是当系统开始
     * 展示广告的时候,它就会记录下广告中包含的哪个单词改善或者损害了广告的
     * 效果,并根据此修改各个可选的定向单词的相对价格.
     *
     * @param locations 地理位置
     * @param content   浏览内容
     * @return 广告定向id, 广告id
     */
    public Pair<Long, String> targetAds(String[] locations, String content) {

        Transaction trans = conn.multi();
        //对所有相关的位置集合执行并集计算操作,产生出最初的一组广告
        //根据用户传入的位置定向参数,找到所有匹配该位置的广告,以及这些广告的eCPM
        String matchedAds = matchLocation(trans, locations);
        //找到存储着所有被匹配广告的集合,以及存储着所有被匹配广告的基本eCPM的有序集合,然后返回它们的id
        String baseEcpm = es.zintersect(trans, 30, new ZParams().weightsByDouble(0, 1), matchedAds, "ad:value:");

        //基于匹配的内容计算附加值
        Pair<Set<String>, String> result = finishScoring(trans, matchedAds, baseEcpm, content);
        //获取一个id,它可以用于汇报并记录这个被定向的广告
        //定向id,这个id代表本次执行的广告定向操作,系统可以通过这个id来追踪广告引发的点击,
        //并从中了解到广告定向操作的那个部分对点击的中数量产生了贡献
        trans.incr("ads:served:");
        //找出eCPM最高的广告,并获取这个广告的id
        trans.zrevrange(es.idxKey(result.getValue1()), 0, 0);

        List<Object> response = trans.exec();
        long targetId = (Long) response.get(response.size() - 2);
        Set<String> targetedAds = (Set<String>) response.get(response.size() - 1);
        //如果没有任何广告与目标位置相匹配,那么返回空值
        if (targetedAds.size() == 0) {
            return new Pair<Long, String>(null, null);
        }

        String adId = targetedAds.iterator().next();
        //记录一系列定向操作的执行结果,作为学习用户行为其中的一个步骤
        recordTargetingResult(targetId, adId, result.getValue0());
        //向调用者返回记录本次定向操作相关信息的id,以及被选中的广告的id
        return new Pair<Long, String>(targetId, adId);
    }

    /**
     * 浏览记录
     *
     * @param targetId 定向id
     * @param adId     广告id
     * @param words    内容
     */
    public void recordTargetingResult(long targetId, String adId, Set<String> words) {
        Set<String> terms = conn.smembers("terms:" + adId);
        String type = conn.hget("type:", adId);
        Transaction trans = conn.multi();
        //找出内容与广告之间相匹配的那些单词
        terms.addAll(words);
        //如果有相匹配的单词出现,就记录它们,并设置15分钟的生存时间
        if (terms.size() > 0) {
            String matchedKey = "terms:matched:" + targetId;
            for (String term : terms) {
                trans.sadd(matchedKey, term);
            }
            trans.expire(matchedKey, 900);
        }

        //记录某种类型广告的展示次数
        trans.incr("type:" + type + ":views");
        //记录广告包含单词的展示次数
        for (String term : terms) {
            trans.zincrby("views:" + adId, 1, term);
        }
        //记录广告的展示次数
        trans.zincrby("views:" + adId, 1, "");

        List<Object> response = trans.exec();
        double views = (Double) response.get(response.size() - 1);
        if (views % 100 == 0) {
            //广告每展示100次就更新一次它的eCPM
        }
    }

    /**
     * 记录点击和动作
     *
     * @param targetId 定向id
     * @param adId     广告id
     * @param action   是否执行动作
     */
    public void recordClick(long targetId, String adId, boolean action) {
        String type = conn.hget("type:", adId);
        Ecpm ecpm = Enum.valueOf(Ecpm.class, type.toUpperCase());

        String clickKey = "clicks:" + adId;
        String matchKey = "terms:matched:" + targetId;
        Set<String> matched = conn.smembers(matchKey);
        matched.add("");

        Transaction trans = conn.multi();
        //如果这是一个按动作计费的广告,并且被匹配的单词仍然存在,那么刷新这些单词的过期时间
        //这使得系统可以在针对目标网站的首次点击通过事件发生之后的15分钟之内,持续对发生的动作进行计数
        if (Ecpm.CPA.equals(ecpm)) {
            trans.expire(matchKey, 900);
            if (action) {
                //记录动作信息而不是点击信息
                clickKey = "actions:" + adId;
            }
        }

        //根据广告的类型,维护一个全局的点击/动作计数器
        if (action && Ecpm.CPA.equals(ecpm)) {
            trans.incr("type:" + type + ":actions:");
        } else {
            trans.incr("type:" + type + ":clicks:");
        }

        //为广告以及所有被定向至该广告的单词记录本次点击(或动作)
        for (String word : matched) {
            trans.zincrby(clickKey, 1, word);
        }
        trans.exec();

        //对广告中出现的所有单词的eCPM进行更新
        //因为广告大约每展示100次至2000次(甚至更多)才会引发一次点击或者动作,
        //所以每次执行这个函数都会调用updateCpms函数
    }

    public void updateCpms(String adId){
        Transaction trans = conn.multi();

    }

    public enum Ecpm {
        CPC, CPA, CPM
    }

    public String idxReqKey(String location) {
        return "idx:req:" + location;
    }

}