package org.demo.chapter06.filedistribution;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.demo.chapter06.messagequeue.ChatMessages;
import org.demo.chapter06.messagequeue.MultipleReceiversMQ;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by lhqz on 2017/8/9.
 */
@Data
public class FileDistribution {

    private Jedis conn;
    private MultipleReceiversMQ multipleReceiversMQ;

    /**
     * 接收日志文件
     * @param id       接收者ID
     * @param callback 处理函数
     */
    public void processLogsFromRedis(String id, Callback callback) throws IOException, InterruptedException {
        while (true) {
            //获取所有群组的消息
            List<ChatMessages> fdata = multipleReceiversMQ.fetchPendingMessages(id);
            //循环所有群组
            for (ChatMessages messages : fdata) {
                //循环群组下的所有消息
                for (Map<String, Object> message : messages.getMessages()) {
                    String logFile = (String) message.get("message");
                    //如果所有的文件都处理完毕,则退出
                    if (Keys.DONE.equals(logFile)) {
                        return;
                    }
                    //如果消息为空,执行下一次循环
                    if (StringUtils.isEmpty(logFile)) {
                        continue;
                    }

                    //从Redis中读取文件
                    InputStream in = new RedisInputStream(conn, messages.getChatId() + logFile);
                    //如果文件是压缩文件
                    if (logFile.endsWith(".gz")) {
                        //用压缩文件流装饰
                        in = new GZIPInputStream(in);
                    }

                    //构建字符流
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try {
                        //读取一行
                        String line = null;
                        //对一行日志进行处理
                        while (StringUtils.isNotEmpty(line = reader.readLine())) {
                            callback.callback(line);
                        }
                        //表示读取到最后一行
                        callback.callback(null);
                    } finally {
                        reader.close();
                    }
                    //文件处理完毕增加文件处理接受者的数量
                    conn.incr(Keys.logFileDoneKey(messages.chatId,logFile));
                }
            }
            //当前没有接收到消息,先等待一会
            if (fdata.size() == 0) {
                Thread.sleep(100);
            }
        }
    }

}