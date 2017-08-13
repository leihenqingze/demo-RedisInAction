package org.demo.chapter07;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by lhqz on 2017/8/12.
 */
public class AdvertisingDirectedTest {

    private static String CONTENT =
            "this is some random content, look at how it is indexed.";

    private Jedis conn;
    private MyES es;
    private AdvertisingDirected ad;

    @Before
    public void init() {
        conn = new Jedis("192.168.1.105");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        es = new MyES();
        es.setConn(conn);
        ad = new AdvertisingDirected();
        ad.setConn(conn);
        ad.setEs(es);
    }

    @Test
    public void testIndexAndTargetAds() {
        System.out.println("\n----- testIndexAndTargetAds -----");
        ad.indexAd("1",new String[]{"USA","CA"},CONTENT, AdvertisingDirected.Ecpm.CPC,.25);
        ad.indexAd("2",new String[]{"USA","VA"},CONTENT + " wooooo", AdvertisingDirected.Ecpm.CPC,.125);
    }

}