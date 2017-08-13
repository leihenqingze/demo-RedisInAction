package org.demo.chapter07;

import com.google.common.collect.Sets;
import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.Set;

/**
 * Created by lhqz on 2017/8/13.
 * 职位搜索
 * <p>
 * 以反向索引的方式查找合适的职位
 * <p>
 * 技能职位集合
 * idx:skill:技能----------set
 * 职位id
 * <p>
 * 职位所需技能数量的集合
 * idx:jobs:req----------zset
 * 职位id     | 所需技能数量
 * <p>
 * 练习
 * 通过技能熟练程度(初学者、中等水平、专家级)
 * 增加技能熟练程度职位集合如下(高级熟练程度集合包含低级熟练集合中的职位):
 * idx:skill:技能:熟练程度----------set
 * 职位id
 * 如果某项技能需要通过熟练程度进行查找时,则求并集时候,
 * 使用这项技能的熟练程度集合与其他技能集合的进行并集操作.
 * <p>
 * 技能经验
 * 增加技能经验职位集合如下:
 * idx:skill:技能:技能经验----------set
 * 职位id
 * 如果某项技能需要通过技能经验进行查找时,则求并集时候,
 * 使用这项技能的经验集合与其他技能集合的进行并集操作.
 * <p>
 * 如果同时通过需要技能的熟练程度和技能经验进行赛选,
 * 则需要对该项技能的熟练程度集合和技能经验集合进行交集操作,
 * 然后再与其他的技能集合进行并集操作.
 */
public class IndexSearchJob {

    @Setter
    private Jedis conn;
    @Setter
    private MyES es;

    /**
     * 根据所需技能对职位进行索引
     *
     * @param jobId  职位id
     * @param skills 职位所需技能
     */
    public void indexJob(String jobId, String... skills) {
        Transaction trans = conn.multi();
        Set<String> unique = Sets.newHashSet();
        //将职位ID添加到相应的技能集合里面
        for (String skill : skills) {
            trans.sadd(skillKey(skill), jobId);
            unique.add(skill);
        }
        //将职位所需技能的数量添加到记录了所有职位所需技能数量的有序集合里面
        trans.zadd(jobsReqKey(), unique.size(), jobId);
        trans.exec();
    }

    /**
     * 找出求职者能够胜任的所有工作
     * 这里的运行速度取决于被搜索职位的数量以及搜索执行的次数,
     * 当职位的数量比较多时候,更是如此.
     * 可以使用分片技术,程序可以将大规模的计算分割为多个小规模的计算,
     * 然后逐步计算出每个小计算的结果.
     * <p>
     * 另外可以把求职者求职的地点作为条件,先从地点集合里面取出位于该地点的所有职位.
     * 然后让技能职位集合与地点集合进行交集运算,然后对各个结果再进行并集运算,减少
     * 搜索职位的数量,提高系统的搜索速度.
     *
     * @param candidateSkills 求职者拥有的技能
     * @return 求职者胜任的职位
     */
    public Set<String> findJobs(String... candidateSkills) {
        String[] keys = new String[candidateSkills.length];
        double[] weights = new double[candidateSkills.length];
        for (int i = 0; i < candidateSkills.length; i++) {
            keys[i] = skill(candidateSkills[i]);
            weights[i] = 1;
        }
        Transaction trans = conn.multi();
        //计算求职者对于每个职位的得分,如果职位在求职者拥有的技能中存在几个,则就是求职者在该职位所需技能的得分
        String jobScores = es.zunion(trans, 30, new ZParams().weightsByDouble(weights), keys);
        //计算出求职者能够胜任以及不能够胜任的职位,
        //求职者在该职位所需技能的得分 * -1 + 职位所需技能数量 * 1 = 0,表示求职者胜任该职位
        String finalResult = es.zintersect(trans, 30, new ZParams().weightsByDouble(-1, 1), jobScores, JOBS_REQ);
        trans.exec();
        //返回求职者能够胜任的那些职位
        Set<String> result = conn.zrangeByScore(es.idxKey(finalResult), 0, 0);
        return result;
    }

    //职位所需技能集合前缀
    private final static String JOBS_REQ = "jobs:req";

    //技能对应职位的集合key
    private String skillKey(String skill) {
        return es.idxKey(skill(skill));
    }

    //技能对应职位的集合前缀
    private String skill(String skill) {
        return "skill:" + skill;
    }

    //职位所需技能数量的集合key
    private String jobsReqKey() {
        return es.idxKey(JOBS_REQ);
    }

}