package org.demo.chapter07;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.demo.commons.Page;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

/**
 * Created by lhqz on 2017/8/12.
 */
public class MyESTest {

    private static String CONTENT =
            "this is some random content, look at how it is indexed.";

    private Jedis conn;
    private MyES es;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.105");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        es = new MyES();
        es.setConn(conn);
    }

    @Test
    public void testIndexDocument() {
        System.out.println("\n----- testIndexDocument -----");
        System.out.println("We're tokenizing some content...");
        Set<String> tokens = es.tokenize(CONTENT);
        System.out.println("Those tokens are: " + Arrays.toString(tokens.toArray()));
        assert tokens.size() == 4;

        System.out.println("And now we are indexing that content...");
        int count = es.indexDocument("test", CONTENT);
        assert count == tokens.size();
        Set<String> test = Sets.newHashSet();
        test.add("test");
        for (String t : tokens) {
            Set<String> members = conn.smembers("idx:" + t);
            assert test.equals(members);
        }

        System.out.println("And now we are rebuild indexing that content...");
        CONTENT = "this is some random content";
        count = es.indexDocument("test", CONTENT);
        assert count == 2;
        Set<String> keys = conn.keys("idx:*");
        assert keys.size() == 2;

    }

    @Test
    public void testSetOperations() {
        System.out.println("\n----- testSetOperations -----");
        es.indexDocument("test", CONTENT);
        Set<String> test = Sets.newHashSet();
        test.add("test");

        Transaction trans = conn.multi();
        String id = es.intersect(trans, 30, "content", "indexed");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = es.intersect(trans, 30, "content", "ignored");
        trans.exec();
        assert conn.smembers("idx:" + id).isEmpty();

        trans = conn.multi();
        id = es.union(trans, 30, "content", "ignored");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = es.difference(trans, 30, "content", "ignored");
        trans.exec();
        assert test.equals(conn.smembers("idx:" + id));

        trans = conn.multi();
        id = es.difference(trans, 30, "content", "indexed");
        trans.exec();
        assert conn.smembers("idx:" + id).isEmpty();
    }

    @Test
    public void testParseQuery() {
        System.out.println("\n----- testParseQuery -----");
        String queryString = "test query without stopwords";
        MyES.Query query = es.parse(queryString);
        String[] words = queryString.split(" ");
        for (int i = 0; i < words.length; i++) {
            List<String> word = Lists.newArrayList();
            word.add(words[i]);
            assert word.equals(query.all.get(i));
        }
        assert query.unwanted.isEmpty();

        queryString = "test +query without -stopwords";
        query = es.parse(queryString);
        assert "test".equals(query.all.get(0).get(0));
        assert "query".equals(query.all.get(0).get(1));
        assert "without".equals(query.all.get(1).get(0));
        assert "stopwords".equals(query.unwanted.toArray()[0]);
    }

    @Test
    public void testParseAndSearch() {
        System.out.println("\n----- testParseAndSearch -----");
        System.out.println("And now we are testing search...");
        es.indexDocument("test", CONTENT);

        Set<String> test = Sets.newHashSet();
        test.add("test");

        String id = es.parseAndSearch("content", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = es.parseAndSearch("content indexed random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = es.parseAndSearch("content +indexed random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = es.parseAndSearch("content indexed +random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        id = es.parseAndSearch("content indexed -random", 30);
        assert conn.smembers("idx:" + id).isEmpty();

        id = es.parseAndSearch("content indexed +random", 30);
        assert test.equals(conn.smembers("idx:" + id));

        System.out.println("Which passed!");
    }

    @Test
    public void testSearchWithSort() {
        System.out.println("\n----- testSearchWithSort -----");
        System.out.println("And now let's test searching with sorting...");
        es.indexDocument("test", CONTENT);
        es.indexDocument("test2", CONTENT);

        Map<String, String> values = Maps.newHashMap();
        values.put("updated", "12345");
        values.put("id", "10");
        conn.hmset("kb:doc:test", values);

        values.put("updated", "54321");
        values.put("id", "1");
        conn.hmset("kb:doc:test2", values);

        MyES.SearchResult result = es.searchAndSort("content", "", 300, "-updated", new Page(1, 10));
        assert "test2".equals(result.results.get(0));
        assert "test".equals(result.results.get(1));

        result = es.searchAndSort("content", "", 300, "-id", new Page(1, 10));
        assert "test".equals(result.results.get(0));
        assert "test2".equals(result.results.get(1));
        System.out.println("Which passed!");
    }

    @Test
    public void testSearchWithZsort() {
        System.out.println("\n----- testSearchWithZsort -----");
        System.out.println("And now let's test searching with sorting via zset...");

        es.indexDocument("test", CONTENT);
        es.indexDocument("test2", CONTENT);

        conn.zadd("idx:sort:update", 12345, "test");
        conn.zadd("idx:sort:update", 54321, "test2");
        conn.zadd("idx:sort:votes", 10, "test");
        conn.zadd("idx:sort:votes", 1, "test2");

        Map<String, Integer> weights = Maps.newHashMap();
        weights.put("update", 1);
        weights.put("vote", 0);
        MyES.SearchResult result = es.searchAndZsort("content", "", 300, false, weights, new Page(1, 10));
        assert "test".equals(result.results.get(0));
        assert "test2".equals(result.results.get(1));

        weights.put("update", 0);
        weights.put("vote", 1);
        result = es.searchAndZsort("content", "", 300, false, weights, new Page(1, 10));
        assert "test2".equals(result.results.get(0));
        assert "test".equals(result.results.get(1));
        System.out.println("Which passed!");
    }

    @Test
    public void testStringToScore() {
        System.out.println("\n----- testStringToScore -----");
        String [] words = "these are some words that will be sorted".split(" ");

        List<MyES.WordScore> pairs = Lists.newArrayList();
        for (String word : words) {
            pairs.add(new MyES.WordScore(word,es.stringToScore(word)));
        }
        List<MyES.WordScore> pairs2 = Lists.newArrayList(pairs);
        Collections.sort(pairs);
        Collections.sort(pairs2, new Comparator<MyES.WordScore>() {
            public int compare(MyES.WordScore o1, MyES.WordScore o2) {
                long diff = o1.score - o2.score;
                return diff < 0 ? -1 : diff > 0 ? 1 : 0;
            }
        });
        assert pairs.equals(pairs2);

    }

}