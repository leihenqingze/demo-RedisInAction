package org.demo.chapter06.filedistribution;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by lhqz on 2017/8/10.
 */
public class TestCallback implements Callback{

    private int index;
    public List<Integer> counts = Lists.newArrayList();

    /**
     * 统计文件行数
     */
    public void callback(String line) {
        System.out.println(line);
        //如果line为null,表示一个文件处理结束
        if (StringUtils.isEmpty(line)){
            index ++;
            return;
        }
        while (counts.size() == index){
            counts.add(0);
        }
        counts.set(index,counts.get(index) + 1);
    }

}
