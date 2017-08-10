package org.demo.chapter06.filedistribution;

/**
 * Created by lhqz on 2017/8/10.
 * 日志文件分发相关key
 */
public class Keys {

    //所有日志文件已经处理完毕的标识
    public final static String DONE = ":done";

    //存储文件内容的key
    public static String logFileKey(String chatId,String logFile){
        return chatId + logFile;
    }

    //存储接收者已处理文件的数量的key
    public static String logFileDoneKey(String chatId,String logFile){
        return logFileKey(chatId,logFile) + DONE;
    }

}
