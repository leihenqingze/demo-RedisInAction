package org.demo.chapter07;

import com.google.common.collect.Sets;
import lombok.Setter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by lhqz on 2017/8/13.
 * 职位搜索
 * <p>
 * 逐个查找合适的职位
 * 1.将职位和职位所需技能存储到Redis中
 * 2.对职位所需技能与求职者拥有的技能是否使用集合差集操作,检查求职者是否胜任该职位
 * 3.获取职位集合,逐个检查职位是否被求职者所胜任
 * <p>
 * 职位集合
 * jobs:----------set
 * 职位id
 * <p>
 * 职位所需技能集合
 * job:jobId----------set
 * 技能
 */
public class SimpleSearchJob {

    @Setter
    private Jedis conn;

    /**
     * 发布职位
     *
     * @param jobId          职位id
     * @param requiredSkills 职位所需的技能
     */
    public void addJob(String jobId, String... requiredSkills) {
        //这里可以使用非事务性流水线,减少往返次数
        //添加职位所需的技能
        conn.sadd(jobSkillsKey(jobId), requiredSkills);
        //将职位添加到职位列表里面
        conn.sadd(JOBS_KEY, jobId);
    }

    /**
     * 查看是否胜任某个职位
     *
     * @param jobId           职位id
     * @param candidateSkills 求职者拥有的技能
     * @return
     */
    public boolean isQualified(String jobId, String... candidateSkills) {
        String temp = UUID.randomUUID().toString();
        Transaction trans = conn.multi();
        //将求职者拥有的技能全部添加到一个临时集合里面,并设置过期时间
//        for (String skill : candidateSkills) {
//            trans.sadd(temp, skill);
//        }
        trans.sadd(temp, candidateSkills);
        trans.expire(temp, 5);
        //找出职位所需技能当中,求职者不具备的那些技能,并将它们记录到结果集合里面
        trans.sdiff(jobSkillsKey(jobId), temp);
        List<Object> response = trans.exec();

        Set<String> diff = (Set<String>) response.get(response.size() - 1);
        //如果SDIFF的计算结果不包含任何技能,那么说明求职者具备职位要求的全部技能
        return diff.size() == 0;
    }

    /**
     * 逐个查找合适的职位
     * 由于职位数量远远大于职位数量,逐个检查毫无疑问是无法进行性能扩展的
     *
     * @param candidateSkills 求职者拥有的技能
     * @return 求职者胜任的职位
     */
    public Set<String> findJobs(String... candidateSkills) {
        //获取所有职位(由于这里使用的是set,所以每次查找得到职位顺序都可能不同,
        //如果需要维持顺序请使用list,如果需要根据某种方式排序请使用zset).
        //这里使用了加载全部职位,在实际中不推荐使用,推荐使用分页的方式进行查找,
        //可以使用zset实现.但是在匹配职位的时候,可能需要对分页进行补齐,需要循
        //环执行职位获取操作.虽然使用分页可以减少执行次数,但是如果求职者与职位
        //的匹配数量很少,仍然需要检查大部分的职位,如:极端情况下,求职者没有合适
        //的职位,我们就需要检查所有的职位.
        //由于这里需要依赖于获取职位集合的结果,所以会有静态条件,如果在匹配职位
        //的时候执行添加操作,这会丢失新的职位,如果删除旧的职位,则会返回失效的
        //职位,不过在这里,职位信息稍有一些数据不一致,是可以接受的,所以就没有执
        //行加锁操作.
        Set<String> jobIds = conn.smembers(JOBS_KEY);
        Set<String> result = Sets.newHashSet();
        //逐个检查求职者是否胜任某个职位,并将胜任的职位添加到结果集合中
        for (String jobId : jobIds) {
            //这里把事务放在了isQualified中,这是应为职位循环太多,由于Redis
            //是基于单线线程事务循环IO模型,如果把执行的操作放在一个大的事务中,
            //会影响其他客户端命令的执行.
            if (isQualified(jobId, candidateSkills)) {
                result.add(jobId);
            }
        }
        return result;
    }

    //职位所需技能的集合key
    private String jobSkillsKey(String jobId) {
        return "job:" + jobId;
    }

    //职位集合的key
    private final static String JOBS_KEY = "jobs:";

}
