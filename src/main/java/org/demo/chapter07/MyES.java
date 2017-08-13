package org.demo.chapter07;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.demo.commons.Page;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Redis可以适合解决基于搜索的问题,这类问题通常需要使用集合以及
 * 有序集合的交、并、差集操作查找符合指定要求的元素.下面展示如何
 * 快速地搜索和过滤数据,并将这些用户组织和搜索自己用户的信息.首
 * 先展示使用Redis进行搜索的方法.Redis自带的集合和有序集合都非
 * 常适合与处理反向索引.
 * <p>
 * <p>
 * 使用Redis进行搜索
 * 建索引
 * <p>
 * 1.分词、语法分析、标记化生成标记、单词
 * 2.去掉非用词、停用词(在文档中频繁出现,却没有提供相应信息量的单词,
 * 移除这些词不仅可以提高搜索性能,还可以减少索引的体积)
 * 3.使用反向索引结构记录索引与文档的关系
 * 搜索
 * 根据多个单词查找文档的话,程序需要把给定单词集合里面的所有文档都找出来,
 * 然后再从中找出那些在所有单词集合里面都出现了的文档.
 * 使用交集操作处理反向索引的好处不在于能都找到多少文档,甚至不在于能够多
 * 快地找出结果,而在于能够彻底地忽略无关的信息.
 * 1.对搜索条件进行语法分析
 * 对搜索结果进行评分和排序
 * <p>
 * 反向索引结构
 * idx:单词----------set
 * 文档id
 * <p>
 * 文档所含索引结构
 * content:word:文档id----------string
 * JSON格式索引集合字符串
 * <p>
 * 文档结构
 * kb:doc:文档id----------hash
 * 文档字段     | 字段值
 * <p>
 * 排序集合结构
 * idx:sort:字段----------zset
 * 文档id | 字段值
 */
public class MyES {

    //用于查找需要的单词、不需要的单词以及同义词的正则表达式
    private final static Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");
    //根据定义提取单词的正则表达式
    private final static Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");
    //预先定义好从http://www.textfixer.com/resources获取的非用词
    //对于文档的用途不同,常用词会不同,所有费非用词也会不同,所以移除非用
    //词的关键是找出合适的非用词清单.所以这里需要扩展,最好抽象出接口对象.
    private final static Set<String> STOP_WORDS = Sets.newHashSet();

    static {
        for (String word : ("able about across after all almost also am among " +
                "an and any are as at be because been but by can " +
                "cannot could dear did do does either else ever " +
                "every for from get got had has have he her hers " +
                "him his how however if in into is it its just " +
                "least let like likely may me might most must my " +
                "neither no nor not of off often on only or other " +
                "our own rather said say says she should since so " +
                "some than that the their them then there these " +
                "they this tis to too twas us wants was we were " +
                "what when where which while who whom why will " +
                "with would yet you your").split(" ")) {
            STOP_WORDS.add(word);
        }
    }

    @Setter
    private Jedis conn;

    /**
     * 分词
     * 该过程会因为语言或者是文档的类型不同处理方式也会不同,
     * 所以需要扩展,最好抽象出接口对象.
     *
     * @param content 文档内容
     * @return 单词集合
     */
    public Set<String> tokenize(String content) {
        //将文档中包含的单词存储到集合中
        Set<String> words = Sets.newHashSet();
        Matcher matcher = WORDS_RE.matcher(content.toLowerCase());
        //遍历文档中包含的所有单词
        while (matcher.find()) {
            String word = matcher.group().trim();
            //保留那些至少有两个字符长的单词,并排除停用词
            //剔除所有位于单词前面或后面的单引号
            word.replace("'", "");
            if (word.length() >= 2 &&
                    !STOP_WORDS.contains(word)) {
                words.add(word);
            }
        }
        //返回一个集合,集合里面包含了所有被保留的不是非用词的单词
        return words;
    }

    /**
     * 创建索引
     *
     * @param docid   文档ID
     * @param content 文档内容
     * @return 索引个数
     */
    public int indexDocument(String docid, String content) {
        //删除文档旧的索引
        deleteIndex(docid);
        //分词,并返回分词结果
        Set<String> words = tokenize(content);
        //将文档添加到正确的反向索引集合里面
        Transaction trans = conn.multi();
        for (String word : words) {
            //这里对索引key的拼接在删除时候也进行了一次,可以在开始时进行一次拼接,
            //用在这里和删除的时候,节省一些时间.由于这里不需要对集合进行修改,如果
            //把处理过程分解成不同的步骤,可以利用Java8中并行流带来的性能提升.
            trans.sadd(idxKey(word), docid);
        }
        trans.set(wordsKey(docid), JSON.toJSONString(words));
        //计算一下,程序为这个文档添加了多少个独一无二的,不是非用词的单词
        return trans.exec().size() - 1;
    }

    /**
     * 删除文档的索引
     * 因为文档的内容变化之后,所需要的索引也会有相应的变化,所以这里也可以用在重建索引之前
     *
     * @param docid 文档id
     */
    public void deleteIndex(String docid) {
        //获取文档中原有的索引(单词),如果不为空,则删除原有的索引
        String oldWordsStr = conn.get(wordsKey(docid));
        if (StringUtils.isNotEmpty(oldWordsStr)) {
            List<String> oldWords = JSON.parseArray(oldWordsStr, String.class);
            if (null != oldWords && oldWords.size() > 0) {
                Transaction trans = conn.multi();
                //循环删除各个索引中存放的文档id,(这里对所有的索引进行了的删除,如果用在重建索引之前
                //可以优化一下,先求出现在在文档用户的单词和原有文档中的差集,然后只需要对差集进行删除就可以了)
                for (String word : oldWords) {
                    trans.srem(idxKey(word), docid);
                }
                trans.exec();
            }
        }
    }

    /**
     * 辅助函数,处理set交、并、差集
     *
     * @param trans  事务对象(使用事务流水线,确保每一个调用都能获得一致的执行结果)
     * @param method 执行的方法名
     * @param ttl    结果的缓存时间
     * @param items  单词集合
     * @return 计算结果集合的key
     */
    private String setCommon(Transaction trans, String method, int ttl, String... items) {
        String[] keys = new String[items.length];
        //获取每个索引集合的key
        for (int i = 0; i < items.length; i++) {
            keys[i] = idxKey(items[i]);
        }
        //创建一个计算结果集合的临时标识符
        String id = UUID.randomUUID().toString();
        try {
            //调用执行的集合操作
            if (method.equals("sinterstore")) {
                trans.sinterstore(idxKey(id), keys);
            } else if (method.equals("sunionstore")) {
                trans.sunionstore(idxKey(id), keys);
            } else if (method.equals("sdiffstore")) {
                trans.sdiffstore(idxKey(id), keys);
            }
            //为将要执行的几个操作设置相应的参数
            //trans.getClass().getDeclaredMethod(method, String.class, String[].class).invoke(trans, idxKey(id), keys);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        //设置计算结果集合的过期时间
        trans.expire(idxKey(id), ttl);
        //返回计算结果集合的key返回给调用者,以便进一步处理
        return id;
    }

    //set交集操作
    public String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sinterstore", ttl, items);
    }

    //set并集操作
    public String union(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sunionstore", ttl, items);
    }

    //set差集操作
    public String difference(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sdiffstore", ttl, items);
    }

    /**
     * 对查询语句进行语法分析
     * 如果不同单词没有加号和减号的前缀表示同时满足的查询条件,是交集的操作.
     * 如果某个单词的前面加了一个减号,表示不查询包含该单词的文档.
     * 如果某个单词的前面加上了一个加号,表示这个单词是前一个单词的同义词.
     * 如果带有加号的单词前面有带减号的单词,那么程序会略过那些带减号的单词,
     * 并把最先遇到的不带减号的单词看做是同义词.
     *
     * @param queryString 查询语句
     * @return 查询对象
     */
    public Query parse(String queryString) {
        Query query = new Query();
        //这个集合用于存储目前已经发现的同义词
        Set<String> current = Sets.newHashSet();
        //提取查询语句中的单词
        Matcher matcher = QUERY_RE.matcher(queryString.toLowerCase());
        //遍历搜索查询语句中的所有单词
        while (matcher.find()) {
            String word = matcher.group().trim();
            //获取单词的前缀
            char prefix = word.charAt(0);
            //检查单词是否带有加号或者减号前缀,如果有的话去掉前缀
            if (prefix == '+' || prefix == '-') {
                word = word.substring(1);
            }
            //删除所有位于单词前面或后面的单引号,并忽略所有非用词
            word.replace("'", "");
            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }
            //如果这是一个不需要的单词,那么将它添加到存储不需要单词的集合里面
            if (prefix == '-') {
                query.unwanted.add(word);
                continue;
            }
            //如果在同义词集合非空的情况下,遇到了一个不带加号前缀的单词,
            //那么创建一个新的同义词集合
            if (!current.isEmpty() && prefix != '+') {
                //把当前的同义词集合添加到交集集合里面
                query.all.add(Lists.newArrayList(current));
                //清空同义词集合
                current.clear();
            }
            //将正在处理的单词添加到同义词集合里面
            current.add(word);
        }
        //把所有剩余的单词都放到最后的交集集合里面进行处理
        if (!current.isEmpty()) {
            query.all.add(Lists.newArrayList(current));
        }
        return query;
    }

    /**
     * 用于分析查询语句并搜索文档的函数
     *
     * @param queryString 查询语句
     * @param ttl         查询结果缓存的时间
     * @return 查询结果集合的key
     */
    public String parseAndSearch(String queryString, int ttl) {
        //对查询语句进行语法分析
        Query query = parse(queryString);
        //如果只有反义词,那么这次搜索将没有任何结果
        if (query.all.isEmpty()) {
            return null;
        }

        List<String> toIntersect = Lists.newArrayList();
        //遍历各个同义词列表
        for (List<String> syn : query.all) {
            //如果同义词列表包含的单词不止一个,那么执行并集计算
            if (syn.size() > 1) {
                Transaction trans = conn.multi();
                toIntersect.add(union(trans, ttl, syn.toArray(new String[syn.size()])));
                trans.exec();
            } else {
                //如果同义词列表只包含一个单词,那么直接使用这个单词
                toIntersect.add(syn.get(0));
            }
        }

        String intersectResult = null;
        //如果单词执行交集计算的结果不止一个,那么执行交集运算
        if (toIntersect.size() > 1) {
            Transaction trans = conn.multi();
            intersectResult = intersect(trans, ttl, toIntersect.toArray(new String[toIntersect.size()]));
            trans.exec();
        } else {
            //如果单词执行交集计算的结果只有一个,那么直接使用这个单词
            intersectResult = toIntersect.get(0);
        }
        //如果用户给定了不需要的单词,那么从交集运算结果里面移除包含这些单词的文档,然后返回搜索结果
        if (!query.unwanted.isEmpty()) {
            String[] keys = query.unwanted.toArray(new String[query.unwanted.size() + 1]);
            keys[keys.length - 1] = intersectResult;
            Transaction trans = conn.multi();
            intersectResult = difference(trans, ttl, keys);
            trans.exec();
        }
        //如果用户没有给定不需要的单词,那么直接返回交集计算的结果作为搜索的结果
        return intersectResult;
    }

    /**
     * 搜索并排序
     * 使用SORT和散列进行排序,适合在元素的排列顺序可以用字符串或者数字表示的情况下使用,
     * 但它并不能处理元素的顺序有几个不同分值组合而成的情况.
     *
     * @param queryString 查询语句
     * @param id          搜索结果标识
     * @param ttl         排序结果缓存时间
     * @param sort        指定搜索结果的排序方式
     * @param page        对结果进行分页
     * @return 搜索结果
     */
    public SearchResult searchAndSort(String queryString, String id,
                                      int ttl, String sort, Page page) {
        boolean desc = false;
        boolean alpha = false;
        if (StringUtils.isNotEmpty(sort)) {
            desc = sort.startsWith("-");
            //判断是进行升序还是降序排序
            if (desc) {
                sort = sort.substring(1);
            }
            //告诉Redis,排序是以数值方式进行还是字母方式进行
            alpha = !"updated".equals(sort) && !"id".equals(sort);
        } else {
            sort = "id";
        }

        //决定基于文档的那个属性进行排序
        String by = contentKey("*->") + sort;

        //如果用户给定了已有的搜索结果,并且这个结果仍然存在的话,那么延长它的生存时间
        if (conn.expire(id, ttl) < 1) {
            id = null;
        }
        //如果用户没有给定已有的搜索结果,或者给定的搜索结果已经过期,
        //那么执行一次新的搜索操作
        if (StringUtils.isEmpty(id)) {
            id = parseAndSearch(queryString, ttl);
        }

        Transaction trans = conn.multi();
        //获取结果集合的元素数量
        trans.scard(idxKey(id));
        //设置排序参数
        SortingParams params = new SortingParams();
        if (desc) {
            params.desc();
        }
        if (alpha) {
            params.alpha();
        }
        params.by(by);
        params.limit(((Long) page.getOffset()).intValue(),
                ((Long) page.getLimit()).intValue());
        //根据指定的属性对结果进行排序,并且只获取用户指定的那一部分结果
        trans.sort(idxKey(id), params);
        List<Object> results = trans.exec();
        //返回搜索结果包含的元素数量、搜索结果本身以及搜索结果的ID,
        //其中搜索结果的ID可以用于在之后再次获取本次搜索的结果
        return new SearchResult(id, ((Long) results.get(0)).longValue(),
                (List<String>) results.get(1));
    }

    /**
     * 辅助函数,处理zset交、并、差集
     *
     * @param trans  事务对象(使用事务流水线,确保每一个调用都能获得一致的执行结果)
     * @param method 执行的方法名
     * @param ttl    结果的缓存时间
     * @param sets   单词集合
     * @return 计算结果集合的key
     */
    private String zsetCommon(Transaction trans, String method,
                              int ttl, ZParams params, String... sets) {
        String[] keys = new String[sets.length];
        //获取每个索引集合的key
        for (int i = 0; i < sets.length; i++) {
            keys[i] = idxKey(sets[i]);
        }

        //创建一个计算结果集合的临时标识符
        String id = UUID.randomUUID().toString();
        try {
            //调用执行的集合操作
            if (method.equals("zinterstore")) {
                trans.zinterstore(idxKey(id), params, keys);
            } else if (method.equals("zunionstore")) {
                trans.zunionstore(idxKey(id), params, keys);
            }
            //为将要执行的几个操作设置相应的参数
            //trans.getClass().getDeclaredMethod(method, String.class, ZParams.class, String[].class).invoke(trans, idxKey(id), params, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //设置计算结果有序集合的过期时间
        trans.expire(idxKey(id), ttl);
        //返回计算结果集合的key返回给调用者,以便进一步处理
        return id;
    }

    //zset交集操作
    public String zintersect(
            Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zinterstore", ttl, params, sets);
    }

    //zset并集操作
    public String zunion(
            Transaction trans, int ttl, ZParams params, String... sets) {
        return zsetCommon(trans, "zunionstore", ttl, params, sets);
    }

    /**
     * 使用有序集合对搜索结果进行排序
     * 可以处理元素的排列顺序有几个不同的分值组合而成的情况
     *
     * @param queryString 查询语句
     * @param id          搜索结果标识
     * @param ttl         排序结果缓存时间
     * @param desc        指定是正序还是倒序排序
     * @param weights     排序集合字段
     * @param page        对结果进行分页
     * @return 搜索结果
     */
    public SearchResult searchAndZsort(String queryString, String id, int ttl,
                                       boolean desc, Map<String, Integer> weights,
                                       Page page) {
        //如果用户给定了已有的搜索结果,并且这个结果仍然存在的话,那么延长它的生存时间
        if (conn.expire(id, ttl) < 1) {
            id = null;
        }
        //如果用户没有给定已有的搜索结果,或者给定的搜索结果已经过期,
        //那么执行一次新的搜索操作
        if (StringUtils.isEmpty(id)) {
            id = parseAndSearch(queryString, ttl);
        }

        int updateWeight = weights.containsKey("update") ? weights.get("update") : 1;
        int voteWeight = weights.containsKey("vote") ? weights.get("vote") : 1;

        String[] keys = new String[]{id, "sort:update", "sort:votes"};
        Transaction trans = conn.multi();
        //使用辅助函数执行交集运算,新集合的分值默认为各个集合分值的和
        id = zintersect(trans, ttl, new ZParams().weightsByDouble(0, updateWeight, voteWeight), keys);
        //获取有序集合的大小
        trans.zcard(idxKey(id));
        //从搜索结果中取出一页
        if (desc) {
            trans.zrevrange(idxKey(id), page.getOffset(), page.getLimit());
        } else {
            trans.zrange(idxKey(id), page.getOffset(), page.getLimit());
        }
        List<Object> results = trans.exec();
        //返回搜素结果,以及分页用的ID值
        return new SearchResult(id, ((Long) results.get(results.size() - 2)).longValue(),
                Lists.newArrayList((Set<String>) results.get(results.size() - 1)));
    }

    public long stringToScore(String string) {
        return stringToScore(string, false);
    }

    /**
     * 将字符串转换为数字分值的函数
     * 因为在Redis里面,有序集合的分值是以IEEE 754双精度浮点数格式存储的,
     * 所以转换操作最大只能使用64个二进制位(8字节),并且由于浮点数格式的某些
     * 细节,转换操作并不能使用全部的64个二进制位.为了简单,下面只使用了48个
     * 二进制位(6字节)进行前缀排序,一般来说这种程度的前缀排序已经足够了.
     * 这种做法对于非数值数据来说基本上是合理的,因为不需要进行大量的计算,也
     * 不必考虑不同语言在传输大整数的时候是否会把整数转为双精度浮点数.
     * <p>
     * 基于字符串生成的分值除了定义排列顺序之外并不具有实际意义,所以它们通常
     * 只会用于单独进行排序,而不会与其他分值一起进行组合排序
     *
     * @param string     前缀字符串
     * @param ignoreCase 是否忽略大小写
     * @return
     */
    public long stringToScore(String string, boolean ignoreCase) {
        if (ignoreCase) {
            string = string.toLowerCase();
        }

        List<Integer> pieces = Lists.newArrayList();
        //将字符串的前6个字符转换为相应的数字值(字符编码)
        for (int i = 0; i < Math.min(string.length(), 6); i++) {
            pieces.add((int) string.charAt(i));
        }
        //为长度不足6个字符的字符串添加占位符,以此来表示这是一个短字符串
        while (pieces.size() < 6) {
            pieces.add(-1);
        }

        long score = 0;
        //对字符串进行转换得出的每个值都会被计算到分值里面,
        //并且程序会以不同的方式处理空字节和占位符
        //ASCII共有256个字符,这里使用了267是为了区分hello\\0和hello,
        //为此(为了区分短字符的填充值和空字节),程序会给所有ASCII值加1(这将是的空字节的ASCII值变为1),
        //使用0(-1 + 1)作为短字符串的填充值
        //乘以字符总数是为了让字符的分值向前移动一个字节
        for (int piece : pieces) {
            score = score * 257 + piece + 1;
        }
        //通过多使用一个二进制位,程序可以表明字符串是否正好为6个字符长,
        //这样它就可以正确地区分出"robber"和"robbers",
        //尽管这对于区分"robbers"和"robbery"并无帮组
        return score * 2 + (string.length() > 6 ? 1 : 0);
    }

    /**
     * 获取给定单词的索引集合的key
     *
     * @param word 单词
     * @return 索引集合key
     */
    public String idxKey(String word) {
        return "idx:" + word;
    }

    /**
     * 获取文档所含单词的key
     *
     * @param docid 文档id
     * @return 文档所含单词的key
     */
    public String wordsKey(String docid) {
        return "content:word:" + docid;
    }

    /**
     * 获取文档散列的key
     *
     * @param docid 文档id
     * @return 文档散列的key
     */
    public String contentKey(String docid) {
        return "kb:doc:" + docid;
    }

    /**
     * 查询对象
     */
    public class Query {
        //外层集合用于存储需要执行交集计算的单词,内层集合用来存储同义词
        public final List<List<String>> all = Lists.newArrayList();
        //用于存储不需要的单词
        public final Set<String> unwanted = Sets.newHashSet();
    }

    /**
     * 查询结果对象
     */
    @AllArgsConstructor
    public class SearchResult {
        //查询结果id
        public final String id;
        //文档数量
        public final long total;
        //文档集合
        public final List<String> results;
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class WordScore
            implements Comparable<WordScore> {

        public final String word;
        public final long score;

        public int compareTo(WordScore other) {
            if (this.word.equals(other.word)) {
                long diff = this.score - other.score;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
            return this.word.compareTo(other.word);
        }
    }

}