package org.demo.chapter08.apistream;

/**
 * Created by lhqz on 2017/8/15.
 * 简单的过滤器演示实现
 */
public class SimpleFilter implements Filter{

    public boolean filter(String line) {
        if (line.equals("test")){
            return true;
        }
        return false;
    }

}
