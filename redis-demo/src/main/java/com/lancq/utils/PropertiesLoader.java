package com.lancq.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/15
 **/
public class PropertiesLoader {
    public PropertiesLoader() {
    }

    public static Properties getProperties(String fileName) throws IOException {
        InputStream inputStream = null;

        Properties properties;
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            inputStream = classLoader.getResourceAsStream(fileName);
            if (inputStream == null) {
                throw new FileNotFoundException(fileName + " not found");
            }

            properties = new Properties();
            properties.load(inputStream);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return properties;
    }

    public static int getIntegerProperty(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : Integer.valueOf(value.trim());
    }

    public static String getStringProperty(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : value.trim();
    }

    public static boolean getBooleanProperty(Properties properties, String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : Boolean.valueOf(value.trim());
    }

    public static void main(String[] args) throws IOException {
        String basePath = System.getProperty("user.dir");
        System.out.println(basePath);
        Properties properties = PropertiesLoader.getProperties("redis.properties");

        if(properties != null){
            System.out.println(properties.get("redis.minIdle"));
            System.out.println(properties.get("redis.port"));


        }
    }
}
