package org.demo.chapter06.filedistribution;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.demo.chapter06.messagequeue.MultipleReceiversMQ;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.Arrays;
import java.util.Deque;
import java.util.Set;

/**
 * Created by lhqz on 2017/8/9.
 * 日志文件分发
 * 1.首先发送日志文件线程会创建群组并将接收者线程加入群组中.
 * 2.然后将日志文件逐个读取到Redis中并发送消息告诉接收者某个日志文件已经保存到Redis中.
 * 3.当所有的日志文件都保存到Redis中之后并发送消息告诉接收者所有的日志文件都已经保存到Redis中.
 * 4.当所有日志文件都保存到Redis中之后,该线程会等待接收者对日志文件进行处理,并对接收者处理完毕的日志文件执行清理操作.
 * <p>
 * 日志文件在Redis中的存储
 * 群组ID(channel) + 日志文件名字----------String(文件内容)
 * 群组ID(channel) + 日志文件名字 + ":done"----------String(完成文件处理的接受者个数)
 * <p>
 * 发送的消息
 * 1.群组创建,初始消息为空
 * 2.某个日志文件已经保存到了Redis,消息内容为日志文件名
 * 3.所有的日志文件都保存到了Redis,消息内容为:done
 */
@Data
@AllArgsConstructor
public class CopyLogsThread extends Thread {

    //消息发送者名称
    private final static String SENDER = "source";

    private Jedis conn;
    private MultipleReceiversMQ multipleReceiversMQ;

    //日志文件路径
    private File path;
    //群组ID
    private String channel;
    //等待处理日志文件的所有客户端数量
    private int count;
    //Redis中可以存放的日志文件总大小限制
    private long limit;

    /**
     * 发送日志文件
     */
    @Override
    public void run() {
        //创建用于向客户端发送消息的群组
        createChat();
        //待处理日志文件队列
        Deque<File> waiting = Queues.newArrayDeque();
        //Redis中所有日志文件的总大小
        long bytesInRedis = 0;
        //获取所有的日志文件
        File[] logFiles = getFiles();
        //遍历发送所有日志文件
        for (File logFile : logFiles) {
            long fsize = logFile.length();
            //判断是否大于了Redis中所有日志文件总大小的限制,如果大于了则先执行清理操作
            while ((bytesInRedis + fsize) > limit) {
                bytesInRedis = clean(bytesInRedis, waiting, count);
            }
            //将日志文件读入到Redis中
            readFile(logFile);
            //提醒监听者,文件已经准备就绪
            multipleReceiversMQ.sendMessage(channel, SENDER, logFile.toString());
            bytesInRedis += fsize;
            waiting.addLast(logFile);
        }
        //所有日志文件已经处理完毕,向监听者报告此事
        multipleReceiversMQ.sendMessage(channel, SENDER, Keys.DONE);
        //等待处理完成的日志文件
        while (waiting.size() > 0) {
            bytesInRedis = clean(bytesInRedis, waiting, count);
        }
    }

    /**
     * 创建用于向客户端发送消息的群组
     */
    private void createChat() {
        Set<String> recipients = Sets.newHashSet();
        for (int i = 0; i < count; i++) {
            recipients.add(String.valueOf(i));
        }
        multipleReceiversMQ.createChat(SENDER, recipients, "", channel);
    }

    /**
     * 获取所有的日志文件
     *
     * @return 日志文件集合
     */
    private File[] getFiles() {
        File[] logFiles = path.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("temp_redis");
            }
        });
        Arrays.sort(logFiles);
        return logFiles;
    }

    /**
     * 将日志文件读入到Redis中
     *
     * @param logFile 日志文件
     */
    private void readFile(File logFile) {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(logFile));
            int read = 0;
            byte[] buffer = new byte[8192];
            while ((read = in.read(buffer, 0, buffer.length)) != -1) {
                if (buffer.length != read) {
                    byte[] bytes = new byte[read];
                    System.arraycopy(buffer, 0, bytes, 0, read);
                    conn.append(Keys.logFileKey(channel,logFile.toString()).getBytes(), bytes);
                } else {
                    conn.append(Keys.logFileKey(channel,logFile.toString()).getBytes(), buffer);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * 在日志文件被所有客户端读取完毕之后,正确地执行清理操作
     *
     * @param bytesInRedis 在Redis中文件的总大小
     * @param waiting      等待处理的日志队列
     * @param count        等待处理日志文件的所有客户端数量
     * @return 清理之后Redis内存中剩余的文件中大小
     */
    private long clean(long bytesInRedis, Deque<File> waiting, int count) {
        //在日志文件被所有客户端读取完毕之后,正确地执行清理操作
        long cleaned = clean(waiting, count);
        if (cleaned != 0) {
            //清理之后减小Redis中文件的总大小
            bytesInRedis -= cleaned;
        } else {
            try {
                //如果没有处理完成的文件,等待一会
                sleep(250);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
        return bytesInRedis;
    }

    /**
     * 在日志文件被所有客户端读取完毕之后,正确地执行清理操作
     *
     * @param waiting 等待处理的日志队列
     * @param count   等待处理日志文件的所有客户端
     * @return 被删除文件的长度
     */
    private long clean(Deque<File> waiting, int count) {
        //如果等待队列中没有数据,直接返回
        if (waiting.size() == 0) {
            return 0;
        }
        //获取队列中的第一个文件
        File w0 = waiting.getFirst();
        //如果该文件的处理客户端等于所有的客户端数量,
        //则表示所有的客户端都对文件进行了处理,可以清理
        String done = Keys.logFileDoneKey(channel, w0.toString());
        if (String.valueOf(count).equals(conn.get(done))) {
            conn.del(Keys.logFileKey(channel, w0.toString()), done);
            //返回删除的文件的长度
            return waiting.removeFirst().length();
        }
        return 0;
    }
}