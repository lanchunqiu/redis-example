package com.lancq.redis;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/16
 **/
public class Test {
    public static void main(String[] args) {
        RedisManager manager = RedisManager.getInstance();
        manager.setEnableReadWriteSeparation(true);
        manager.set("test","123");
        manager.get("test");

//        for(int i = 0; i < 10; i++){
//            manager.set("test",i+"");
//        }
//
//        for(int i = 0; i < 10; i++){
//            manager.get("test");
//        }
//
//        for(int i = 0; i < 10; i++){
//            manager.set("test",i+"");
//        }
    }
}
