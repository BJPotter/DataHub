package com.vasoyn.handle;

import com.vasoyn.config.GlobalConfig;
import com.vasoyn.utils.TimeUtil;
import java.util.Map;

/**
 * Author: Zhi Liu
 * Date: 2024/1/9 16:41
 * Contact: liuzhi0531@gmail.com
 * Desc:
 */
public class TopicHandleProcessor {
    public static String processContentBasedOnTopic(String topic, String content,Map<String,String> filter,Map<String,String> add,Map<String,String> split) {
        String filterKey = topic+".filter";
        String addKey = topic+".add";
        String splitKey = topic+".split";
        if(filter.containsKey(filterKey)){
            String[] filterValue = filter.get(filterKey).split("\\|");
            for (int i = 0; i < filterValue.length; i++) {
                if(content.contains(filterValue[i])){
                    return null;
                }
            }
        }
        if(add.containsKey(addKey)){
            String value = add.get(addKey);
            if(value.equals("DateTime")){
                return TimeUtil.getDateTime()+split.get(splitKey)+content;
            }
            if(value.equals("Date")){
                return TimeUtil.getDate()+split.get(splitKey)+content;
            }
        }
        return content;
    }
}
