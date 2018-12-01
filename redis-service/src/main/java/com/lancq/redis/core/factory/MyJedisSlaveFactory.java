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
 * @Description
 * @Date 2018/9/16
 **/
public class MyJedisSlaveFactory implements PooledObjectFactory<Jedis> {
    private MyJedisFactory masterFactory;

    private MyJedisPool pool;

    private List<MyJedisInfo> jedisInfoList;

    private MyJedis currentSlave;
    private int database;

    Logger logger = LoggerFactory.getLogger(MyJedisSlaveFactory.class);

    public MyJedisSlaveFactory(List<MyJedisInfo> jedisInfoList, MyJedisFactory masterFactory, MyJedisPool pool){
        this.jedisInfoList = jedisInfoList;
        this.masterFactory = masterFactory;
        this.pool = pool;
        this.database = 0;

    }


    //必须是主节点且是活动状态
    @Override
    public boolean validateObject(PooledObject obj) {
        try{
            Jedis jedis = (Jedis) obj;
            return currentSlave.equals(jedis) && checkIsAlive(jedis);
        } catch (Exception e){
            logger.error(e.getMessage());
            return false;
        }
    }



    @Override
    public PooledObject<Jedis> makeObject() throws Exception {
        if (currentSlave == null || !checkIsAlive(currentSlave)){
            switchIp();
        }

        final MyJedis jedis = new MyJedis(currentSlave.getIp(), currentSlave.getPort(), currentSlave.getTimeout());
        try {
            jedis.connect();
        } catch (JedisException je) {
            jedis.close();
            throw je;
        }
        return new DefaultPooledObject<Jedis>(jedis);
    }

    @Override
    public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
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

    private boolean checkIsAlive(Jedis jedis){

        return masterFactory.checkIsAlive(jedis);
    }

    private boolean checkIsAlive(String ip, int port){
        return masterFactory.checkIsAlive(ip,port);
    }
    private synchronized void findBaseSlave(){
        try{
            boolean finded = false;
            for (MyJedisInfo ha : jedisInfoList){
                //第一次选时
                if (currentSlave == null){
                    if (!ha.isMaster()
                            && !masterFactory.getCurrentMaster().equals(ha.getIp(), ha.getPort())
                            && checkIsAlive(ha.getIp(),ha.getPort())){
                        currentSlave = new MyJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
                        finded = true;
                        break;
                    }
                }
                //后继选择时
                if (!masterFactory.getCurrentMaster().equals(ha.getIp(), ha.getPort())
                        && checkIsAlive(ha.getIp(),ha.getPort())){
                    this.destroyObject((PooledObject<Jedis>) currentSlave);
                    currentSlave = new MyJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
                    finded = true;
                    break;
                }
            }
            if (!finded) {
                //一次未找到可用的后，则不再尝试检测从服务器的的状态，直接使用主服务器
                logger.warn("未找到可用的从服务器,切换到主服务器服务!");
                currentSlave = masterFactory.getCurrentMaster();
                logger.warn("切换到的主服务器:"+currentSlave);
            }
        }catch(Exception e){
            logger.error(e.getMessage(),e);
        }
    }

    private void switchIp(){
        pool.destorySlaveAll();
        findBaseSlave();
        logger.info("switchToSlave:"+currentSlave);
    }
}
