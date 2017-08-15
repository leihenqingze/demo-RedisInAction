package org.demo.chapter08.mytwitter;

import lombok.AllArgsConstructor;
import java.lang.reflect.Method;

/**
 * Created by lhqz on 2017/8/15.
 * 延迟任务调用线程类
 */
@AllArgsConstructor
public class MethodThread extends Thread {

    private Object instance;
    private Method method;
    private Object[] args;

    @Override
    public void run() {
        try {
            method.invoke(instance,args);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

}