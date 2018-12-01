package com.lancq.redis.core;

import com.lancq.redis.core.factory.MyJedisFactory;
import com.lancq.redis.core.factory.MyJedisSlaveFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/16
 **/
public class MyJedisPool{
    //支持读写分离
    private GenericObjectPool<Jedis> masterPool = null;

    private GenericObjectPool<Jedis> slavePool = null;

    public MyJedisPool(JedisPoolConfig config, String ip, int port, int timeout){
        List<MyJedisInfo> jedisInfoList = new ArrayList<MyJedisInfo>();
        MyJedisInfo jedisInfo = new MyJedisInfo(ip, port, timeout);
        jedisInfo.setMaster(true);
        jedisInfoList.add(jedisInfo);
        MyJedisFactory jedisFactory = new MyJedisFactory(jedisInfoList,this);
        this.masterPool = new GenericObjectPool<Jedis>(jedisFactory, config);
        this.slavePool = new GenericObjectPool<Jedis>(new MyJedisSlaveFactory(jedisInfoList, jedisFactory,this), config);
    }

    public MyJedisPool(JedisPoolConfig config, List<MyJedisInfo> jedisInfoList){
        MyJedisFactory jedisFactory = new MyJedisFactory(jedisInfoList, this);
        this.masterPool = new GenericObjectPool<Jedis>(jedisFactory, config);
        this.slavePool = new GenericObjectPool<Jedis>(new MyJedisSlaveFactory(jedisInfoList, jedisFactory,this), config);
    }

    public Jedis getResource(){
        try {
            Object object = masterPool.borrowObject();
            return (Jedis) object;
        } catch (Exception e) {
            e.printStackTrace();
            throw new JedisConnectionException("获取jedis连接失败" + e);
        }
    }

    public Jedis getSlaveResource(){
        try {
            return (Jedis) slavePool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new JedisConnectionException("获取jedis连接失败" + e);
        }
    }

    public void returnResource(final Jedis jedis) {
        try {
            masterPool.returnObject(jedis);
        } catch (Exception e) {
            throw new JedisException("回收jedis连接失败", e);
        }
    }

    public void returnSlaveResource(final Jedis jedis) {
        try {
            slavePool.returnObject(jedis);
        } catch (Exception e) {
            throw new JedisException("回收jedis连接失败", e);
        }
    }

    public void destoryMasterAll(){
        if (masterPool != null){
            masterPool.clear();
        }
    }
    public void destorySlaveAll(){
        if (slavePool != null){
            slavePool.clear();
        }
    }
}
