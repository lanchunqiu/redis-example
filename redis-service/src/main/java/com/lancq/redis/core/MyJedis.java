package com.lancq.redis.core;

import redis.clients.jedis.Jedis;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/16
 **/
public class MyJedis extends Jedis {

    public MyJedis(String host) {
        super(host);
    }

    public MyJedis(String host, int port) {
        super(host, port);
    }

    public MyJedis(String host, int port, int timeout) {
        super(host, port, timeout);
    }

    /**
     * 判断jedis对象是否相等
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;

        Jedis jedis = (Jedis) obj;

        return this.getClient().getHost().equals(jedis.getClient().getHost())
                && this.getClient().getPort() == jedis.getClient().getPort();
    }

    public boolean equals(String host, int port){
        return this.getClient().getHost().equals(host)
                && this.getClient().getPort() == port;
    }

    @Override
    public String toString() {
        return this.getClient().getHost() + ":" + this.getClient().getPort();
    }

    public String getIp(){
        return this.getClient().getHost();
    }
    public int getPort(){
        return this.getClient().getPort();
    }
    public int getTimeout(){
        return this.getClient().getSoTimeout();
    }
}
