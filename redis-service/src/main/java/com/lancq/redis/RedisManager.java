package com.lancq.redis;

import redis.clients.jedis.Jedis;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/16
 **/
public class RedisManager {
    private RedisManagerProxy redisManagerProxy;
    /**
     * 连接ip格式，默认第一个为主服务器,后面的为从服务器,若只设置一个，则为主服务器
     */
    public static final String DEFAULTIPFORMAT = "127.0.0.1:6379,127.0.0.1:6380";

    ThreadLocal<OP> cop = new ThreadLocal<OP>();

    boolean enableReadWriteSeparation = false;//是否允许读写分离

    /**
     * 使用CGlib动态代理，被代理对象必须有一个无参的构造方法
     */
    public RedisManager() {
        super();
    }
    private RedisManager(RedisManagerProxy proxy){
        this.redisManagerProxy = proxy;
    }

    /**
     * 安全创建单例对象
     */
    private static class InstanceHolder{
        static final RedisManagerProxy proxy = new RedisManagerProxy();
        static final RedisManager instance = (RedisManager) proxy.getProxy(new RedisManager(proxy));
    }


    public static RedisManager getInstance(){
        return InstanceHolder.instance;
    }

    public void setEnableReadWriteSeparation(boolean enable) {
        enableReadWriteSeparation = enable;
    }

    private void initOP(OP op){
        if(enableReadWriteSeparation){
            cop.set(op);
        } else {
            cop.set(OP.READ_OR_WRITE);
        }
    }

    public ThreadLocal<OP> getCop() {
        return cop;
    }

    public void setCop(ThreadLocal<OP> cop) {
        this.cop = cop;
    }

    ///////////针对单字符串的操作//////////
    public void del(String key) {
        initOP(OP.WRITE);
        redisManagerProxy.getCurrJedis().del(key);
    }

    public void set(String key, String value) {
        initOP(OP.WRITE);
        redisManagerProxy.getCurrJedis().set(key, value);
    }

    public void set(String key, String value, int timeout) {
        initOP(OP.WRITE);
        Jedis jedis = redisManagerProxy.getCurrJedis();
        jedis.set(key, value);
        jedis.expire(key, timeout);
    }

    public String get(String key) {
        initOP(OP.READ);
        return  redisManagerProxy.getCurrJedis().get(key);
    }


}
