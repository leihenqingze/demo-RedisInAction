package org.demo.chapter06.filedistribution;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by lhqz on 2017/8/10.
 */
public class DailyCountryAggregate implements Callback {

    private Jedis conn;
    private Map<String, Integer> countries = Maps.newHashMap();
    private String day;

    public void callback(String line) {
        if (StringUtils.isNotEmpty(line)) {
            String[] tokens = line.split(" ");
            String ip = tokens[0];
            day = tokens[1];
            Integer count = countries.get(ip);
            if (null == count) {
                count = 0;
            }
            count++;
            countries.put(ip, count);
        }

        for (Map.Entry<String, Integer> country : countries.entrySet()) {
            conn.zadd("daily:country:" + day, country.getValue(), country.getKey());
        }
        countries.clear();
    }

}
