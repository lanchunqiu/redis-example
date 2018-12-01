package com.lancq.redis.core.factory;

import com.lancq.redis.core.MyJedis;
import com.lancq.redis.core.MyJedisInfo;
import com.lancq.redis.core.MyJedisPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;

/**
 * @Author lancq
 * @Description 连接池主工厂
 * @Date 2018/9/16
 **/
public class MyJedisFactory implements PooledObjectFactory<Jedis> {
    private MyJedisPool pool;

    private List<MyJedisInfo> jedisInfoList;

    private MyJedisInfo currentJedisInfo;

    private volatile boolean isMaster;

    private MyJedis jedisMaster;

    private MyJedis currentMaster;

    private int database;

    Logger logger = LoggerFactory.getLogger(MyJedisFactory.class);

    public MyJedisFactory(List<MyJedisInfo> jedisInfoList, MyJedisPool pool){
        this.pool = pool;
        this.jedisInfoList = jedisInfoList;

        boolean finded = false;
        for(MyJedisInfo info : jedisInfoList){
            if(info.isMaster()){
                jedisMaster = new MyJedis(info.getIp(), info.getPort(), info.getTimeout());
                currentMaster = jedisMaster;
                isMaster = true;
                currentJedisInfo = info;
                database = currentJedisInfo.getDatabase();
                finded = true;
                break;
            }
        }
        if(!finded || !checkIsAlive(jedisMaster)){
            logger.warn("未设置主服务器或主服务器当前不可用!");
        }
    }

    public boolean checkIsAlive(Jedis jedis){
        try{
            return jedis.ping().equals("PONG");
        } catch (Exception e){
            logger.error(e.getMessage());
            jedis.disconnect();
        }


        return false;
    }

    public boolean checkIsAlive(String ip, int port){
        MyJedis jedis = null;
        try{
            jedis = new MyJedis(ip,port);
            return jedis.ping().equals("PONG");
        }catch(Exception e){
            logger.error(e.getMessage());
        }finally{
            this.destroyObject((PooledObject<Jedis>) jedis);
        }
        return false;
    }

    @Override
    public void destroyObject(PooledObject<Jedis> pooledJedis){
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {

            }
        }
    }

    //必须是主节点且是活动状态
    @Override
    public boolean validateObject(PooledObject<Jedis> pooledJedis) {
        try{
            Jedis jedis = pooledJedis.getObject();
            return currentMaster.equals(jedis) && checkIsAlive(jedis);
        } catch (Exception e){
            logger.error(e.getMessage());
            return false;
        }
    }

    @Override
    public PooledObject<Jedis> makeObject() throws Exception {
        final MyJedis jedis = new MyJedis(currentJedisInfo.getIp(), currentJedisInfo.getPort(), currentJedisInfo.getTimeout());
        try {
            jedis.connect();
        } catch (JedisException je) {
            jedis.close();
            throw je;
        }
        return new DefaultPooledObject<Jedis>(jedis);
    }

    @Override
    public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.getDB() != database) {
            jedis.select(database);
        }
    }

    @Override
    public void passivateObject(PooledObject<Jedis> pooledObject) throws Exception {

    }
    public MyJedisInfo getCurrentJedisInfo() {
        return currentJedisInfo;
    }

    public void setCurrentJedisInfo(MyJedisInfo currentJedisInfo) {
        this.currentJedisInfo = currentJedisInfo;
    }

    public MyJedis getJedisMaster() {
        return jedisMaster;
    }

    public void setJedisMaster(MyJedis jedisMaster) {
        this.jedisMaster = jedisMaster;
    }

    public MyJedis getCurrentMaster() {
        return currentMaster;
    }

    public void setCurrentMaster(MyJedis currentMaster) {
        this.currentMaster = currentMaster;
    }


}
