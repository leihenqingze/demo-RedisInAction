package org.demo.chapter06;

import org.demo.chapter06.filedistribution.CopyLogsThread;
import org.demo.chapter06.filedistribution.FileDistribution;
import org.demo.chapter06.filedistribution.TestCallback;
import org.demo.chapter06.messagequeue.MultipleReceiversMQ;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * Created by lhqz on 2017/8/5.
 */
public class FileDistributionTest {

    private Jedis conn;
    private FileDistribution fileDistribution;
    private MultipleReceiversMQ mq;

    @Before
    public void init() {
        conn = new Jedis("192.168.0.115");
        conn.auth("123456");
        conn.select(6);
        conn.flushDB();

        DistributedLock lock = new DistributedLock();
        lock.setConn(conn);
        mq = new MultipleReceiversMQ();
        mq.setConn(conn);
        mq.setLock(lock);

        fileDistribution = new FileDistribution();
        fileDistribution.setConn(conn);
        fileDistribution.setMultipleReceiversMQ(mq);

    }

    @Test
    public void testFileDistribution() throws IOException, InterruptedException {
        System.out.println("\n----- testFileDistribution -----");
        System.out.println("Creating some temporary 'log' files...");
        File f1 = File.createTempFile("temp_redis_1_",".txt");
        System.out.println(f1.getAbsolutePath());
        System.out.println(f1.toString());
        f1.deleteOnExit();
        Writer writer = new FileWriter(f1);
        writer.write("one line\n");
        writer.close();

        File f2 = File.createTempFile("temp_redis_2_",".txt");
        f2.deleteOnExit();
        writer = new FileWriter(f2);
        for (int i = 0; i < 100; i++) {
            writer.write("many lines " + i + "\n");
        }
        writer.close();

        File f3 = File.createTempFile("temp_redis_3_",".txt.gz");
        f3.deleteOnExit();
        writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(f3)));
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            writer.write("random line " + Long.toHexString(random.nextLong()) + "\n");
        }
        writer.close();

        long size = f3.length();
        System.out.println("Done.");
        System.out.println();

        System.out.println("Starting up a thread to copy logs to redis...");
        File path = f1.getParentFile();
        CopyLogsThread thread = new CopyLogsThread(conn,mq,path,"test",1,size);
        thread.start();

        System.out.println("Let's pause to let some logs get copied to Redis...");
        Thread.sleep(250);
        System.out.println();
        System.out.println("Okay, the logs should be ready. Let's process them!");

        System.out.println("Files should have 1, 100, and 1000,lines");
        TestCallback callback = new TestCallback();
        fileDistribution.processLogsFromRedis("0",callback);
        System.out.println(Arrays.toString(callback.counts.toArray(new Integer[0])));
        assert callback.counts.get(0) == 1;
        assert callback.counts.get(1) == 100;
        assert callback.counts.get(2) == 1000;
        System.out.println();

        System.out.println("Let's wait for the copy thread to finish cleaning up...");
        thread.join();
        System.out.println("Done cleaning out Redis!");

    }

    @After
    public void destroy() {
        conn.flushDB();
    }

}