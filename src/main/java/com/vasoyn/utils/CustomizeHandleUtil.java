package com.vasoyn.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Author: Zhi Liu
 * Date: 2024/1/10 17:10
 * Contact: liuzhi0531@gmail.com
 * Desc:
 */
public class CustomizeHandleUtil {
    private static final Logger logger = LoggerFactory.getLogger(CustomizeHandleUtil.class);
    public static String getOtherJar(String pathToJar, String topic,String argument){
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", pathToJar, topic,argument);
            Process process = processBuilder.start();
            // 读取子进程的输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String output = reader.readLine();
            // 等待子进程完成
            process.waitFor();
            return output;
        } catch (Exception e) {
            logger.error("自定义处理调用失败",e);
            return null;
        }
    }
}
