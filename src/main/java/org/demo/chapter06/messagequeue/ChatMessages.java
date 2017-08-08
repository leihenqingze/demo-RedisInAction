package org.demo.chapter06.messagequeue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by lhqz on 2017/8/8.
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class ChatMessages implements Serializable {

    public String chatId;
    public List<Map<String, Object>> messages;

}