package org.demo.chapter06.filedistribution;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by lhqz on 2017/8/10.
 * 读取Redis中的文件流
 */
public class RedisInputStream extends InputStream {

    private Jedis conn;
    private String key;
    private int pos;

    public RedisInputStream(Jedis conn,String key){
        this.conn = conn;
        this.key = key;
    }

    public int read() throws IOException {
        byte[] block = conn.substr(key.getBytes(), pos, pos);
        if (null == block || block.length == 0) {
            return -1;
        }
        pos++;
        return (block[0] & 0xff);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
        if (null == block || block.length == 0) {
            return -1;
        }
        System.arraycopy(block, 0, buf, off, block.length);
        pos += block.length;
        return block.length;
    }

    /**
     * 是否可读
     * @return
     * @throws IOException
     */
    @Override
    public int available() throws IOException {
        long len = conn.strlen(key);
        return (int) (len - pos);
    }

    @Override
    public void close() throws IOException {

    }

}