package com.lancq.redis;

import com.lancq.utils.PropertiesLoader;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.*;

/**
 * @Author lancq
 * @Description
 * @Date 2018/9/15
 **/
public class RedisPoolClient {

    private static volatile RedisPoolClient instance;
    private JedisPool jedisPool;
    private List<JedisPool> jedisPoolSlaves;
    private volatile boolean enableReadWriteSeparation = false;//是否允许读写分离

    public static final String REDIS_MASTER_ADDR = "redis.master.addr";
    public static final String REDIS_SLAVES_ADDR = "redis.slaves.addr";
    public static final String REDIS_MAX_IDLE = "redis.maxIdle";
    public static final String REDIS_MAX_TOTAL = "redis.maxTotal";
    public static final String REDIS_MAX_WAIT_MILLIS = "redis.maxWaitMillis";
    public static final String REDIS_MIN_IDLE = "redis.minIdle";
    public static final String REDIS_TEST_ON_BORROW = "redis.testOnBorrow";
    public static final String REDIS_TEST_ON_RETURN = "redis.testOnReturn";
    public static final String REDIS_TEST_WHILE_IDLE = "redis.testWhileIdle";
    public static final String REDIS_TIME_BETWEEN_ERM = "redis.timeBetweenEvictionRunsMillis";
    public static final String REDIS_MIN_ETM = "redis.minEvictableIdleTimeMillis";
    public static final String REDIS_CONNECTION_TIMEOUT = "redis.connectionTimeout";
    public static final String REDIS_PASSWORD = "redis.password";

    private RedisPoolClient() {
        Properties properties = null;

        try {
            properties = PropertiesLoader.getProperties("redis.properties");
        } catch (IOException e) {
            System.out.println("=========取redis.properties文件异常:" + e.toString());
        }
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(150);
        jedisPoolConfig.setMaxTotal(600);
        jedisPoolConfig.setMaxWaitMillis(10000L);
        jedisPoolConfig.setMinIdle(30);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000L);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(60000L);

        if(properties != null){
            if (properties.get(REDIS_MAX_IDLE) != null) {
                jedisPoolConfig.setMaxIdle(PropertiesLoader.getIntegerProperty(properties, REDIS_MAX_IDLE, 150));
            }

            if (properties.get(REDIS_MAX_TOTAL) != null) {
                jedisPoolConfig.setMaxTotal(PropertiesLoader.getIntegerProperty(properties, REDIS_MAX_TOTAL, 600));
            }

            if (properties.get(REDIS_MAX_WAIT_MILLIS) != null) {
                jedisPoolConfig.setMaxWaitMillis((long)PropertiesLoader.getIntegerProperty(properties, REDIS_MAX_WAIT_MILLIS, 10000));
            }

            if (properties.get(REDIS_MIN_IDLE) != null) {
                jedisPoolConfig.setMinIdle(PropertiesLoader.getIntegerProperty(properties, REDIS_MIN_IDLE, 30));
            }

            if (properties.get(REDIS_TEST_ON_BORROW) != null) {
                jedisPoolConfig.setTestOnBorrow(PropertiesLoader.getBooleanProperty(properties, REDIS_TEST_ON_BORROW, true));
            }

            if (properties.get(REDIS_TEST_ON_RETURN) != null) {
                jedisPoolConfig.setTestOnReturn(PropertiesLoader.getBooleanProperty(properties, REDIS_TEST_ON_RETURN, true));
            }

            if (properties.get(REDIS_TEST_WHILE_IDLE) != null) {
                jedisPoolConfig.setTestWhileIdle(PropertiesLoader.getBooleanProperty(properties, REDIS_TEST_WHILE_IDLE, true));
            }

            if (properties.get(REDIS_TIME_BETWEEN_ERM) != null) {
                jedisPoolConfig.setTimeBetweenEvictionRunsMillis((long)PropertiesLoader.getIntegerProperty(properties, REDIS_TIME_BETWEEN_ERM, 30000));
            }

            if (properties.get(REDIS_MIN_ETM) != null) {
                jedisPoolConfig.setMinEvictableIdleTimeMillis((long)PropertiesLoader.getIntegerProperty(properties, REDIS_MIN_ETM, 60000));
            }
        }
        //初始化redis主节点
        if (jedisPool == null) {
            String redis_master_address = PropertiesLoader.getStringProperty(properties, REDIS_MASTER_ADDR, null);
            System.out.println(redis_master_address);

            String[] master = redis_master_address.split(":");
            String master_host = master[0];
            String master_port = master[1];
            int port = Integer.parseInt(master_port);
            System.out.println(port);

            int cnout = PropertiesLoader.getIntegerProperty(properties, REDIS_CONNECTION_TIMEOUT, 10000);
            System.out.println(cnout);

            jedisPool = new JedisPool(jedisPoolConfig, master_host, port, cnout, "asopa_redis");
            System.out.println("jedisPoolMaster:" + jedisPool);
        }

        //初始化redis从节点
        if(jedisPoolSlaves == null || jedisPoolSlaves.size() <= 0){
            String redis_slaves_address = PropertiesLoader.getStringProperty(properties, REDIS_SLAVES_ADDR, null);
            System.out.println(redis_slaves_address);
            if(redis_slaves_address != null){
                String[] slaves = redis_slaves_address.split(",");//多个slave已逗号分隔：192.168.227.130:6379,192.168.227.131:6379
                jedisPoolSlaves = new ArrayList<JedisPool>();
                int cnout = PropertiesLoader.getIntegerProperty(properties, REDIS_CONNECTION_TIMEOUT, 10000);
                System.out.println(cnout);

                for(String salve : slaves){
                    System.out.println(salve);
                    String host = salve.split(":")[0];
                    String port = salve.split(":")[1];

                    JedisPool temp = new JedisPool(jedisPoolConfig, host, Integer.parseInt(port), cnout, "asopa_redis");
                    jedisPoolSlaves.add(temp);
                }
            }
            System.out.println("jedisPoolSlaves.size:" + jedisPoolSlaves.size());
        }
    }
    public static RedisPoolClient getInstance(){
        if(instance == null){
            synchronized (RedisPoolClient.class){
                if(instance == null){
                    instance = new RedisPoolClient();
                }
            }
        }
        return instance;
    }

    public boolean isEnableReadWriteSeparation() {
        return enableReadWriteSeparation;
    }

    public void setEnableReadWriteSeparation(boolean enableReadWriteSeparation) {
        this.enableReadWriteSeparation = enableReadWriteSeparation;
    }

    /**
     * 根据读写分离配置获取JedisPool
     * @return
     */
    public JedisPool getSlavePool(){
        JedisPool jedisPool = null;
        if(this.enableReadWriteSeparation){
            //从slave服务中取jedisPool
            int index = new Random().nextInt(jedisPoolSlaves.size());
            jedisPool = jedisPoolSlaves.get(index);
            System.out.println("slave[" + index + "]");
        } else {
            jedisPool = this.jedisPool;
            System.out.println("master");
        }

        System.out.println("jedisPool.isClosed = [" + jedisPool.isClosed() + "]");
        System.out.println("jedisPool.NumActive = [" + jedisPool.getNumActive() + "]");
        System.out.println("jedisPool.NumIdle = [" + jedisPool.getNumIdle() + "]");
        System.out.println("jedisPool.NumWaiters = [" + jedisPool.getNumWaiters() + "]");

        return jedisPool;
    }

    /**
     *
     * @param key 键
     * @param value 键
     */
    public void set(String key, String value) {

        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            jedis.set(key, value);
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
    }

    /**
     *
     * @param key 键
     * @param seconds 过期时间
     * @param value 值
     */
    public void set(String key, int seconds, String value) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            jedis.setex(key, seconds, value);
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

    }

    /**
     *
     * @param key 键
     * @return
     */
    public String get(String key) {
        Jedis jedis = null;

        JedisPool jedisPool = this.getSlavePool();
        try {
            jedis = jedisPool.getResource();
            String value = jedis.get(key);
            return value;
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return null;
    }

    /**
     * 向有序集合中添加元素
     * @param key 键
     * @param score 分数
     * @param member 成员
     * @return 1添加成功 0成员已经存在并且分数已更新 -1操作失败
     */
    public long zadd(String key, Double score, String member) {
        Jedis jedis = null;
        long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zadd(key, score, member);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return result;
    }

    public Set<String> zrange(String key, long start, long end) {
        Jedis jedis = null;

        JedisPool jedisPool = this.getSlavePool();

        Set set;
        try {
            jedis = jedisPool.getResource();
            set = jedis.zrange(key, start, end);
            return set;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            set = new HashSet();
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return set;
    }

    public Set<Tuple> zrangeWithSocres(String key, long start, long end) {
        Jedis jedis = null;

        JedisPool jedisPool = this.getSlavePool();

        Set<Tuple> set;
        try {
            jedis = jedisPool.getResource();
            set = jedis.zrangeWithScores(key, start, end);
            return set;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            set = new HashSet();
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return set;
    }

    /**
     * 返回有序集合的记录数
     * @param key
     * @return
     */
    public long zcard(String key){
        Jedis jedis = null;

        JedisPool jedisPool = this.getSlavePool();

        long result = 0L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zcard(key);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = 0L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 返回有序集合记录的排位数
     * @param key
     * @param memeber
     * @return
     */
    public long zrank(String key, String memeber){
        Jedis jedis = null;

        JedisPool jedisPool = this.getSlavePool();

        long result = -1L;
        try {
            jedis = jedisPool.getResource();
            Object obj = jedis.zrank(key, memeber);
            System.out.println("obj:" + obj);
            if(obj != null){
                result = (Long)obj;
            }
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }
    public Set<String> zrangeByScore(String key, double min, double max) {
        Jedis jedis = null;
        JedisPool jedisPool = this.getSlavePool();
        Set set;
        try {
            jedis = jedisPool.getResource();
            set = jedis.zrangeByScore(key, min, max);
            return set;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            set = new HashSet();
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return set;
    }
    /**
     * 删除键key及其对应的数据
     * @param key
     * @return
     */
    public Long del(String key){
        Jedis jedis = null;
        long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.del(key);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return result;
    }
    /**
     * 从有序集合中删除元素
     * @param key 键
     * @param member 成员
     * @return 1删除成功 0成员不存在 -1操作失败
     */
    public Long zrem(String key, String member) {
        //System.out.println("有序集合" + key + "中删除元素" + member);
        Jedis jedis = null;

        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zrem(key, member);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }
    /**
     * 从有序集合中删除元素
     * @param key 键
     * @param members 成员
     * @return 1删除成功 0成员不存在 -1操作失败
     */
    public Long zrem(String key, String... members) {
        Jedis jedis = null;

        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zrem(key, members);
            //System.out.println("有序集合" + key + "中删除元素" + members);
            //System.out.println("删除结果" + result);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 判断key是否存在
     * @param key 键
     * @return
     */
    public Boolean exists(String key){
        Jedis jedis = null;
        JedisPool jedisPool = this.getSlavePool();
        Boolean result = false;
        try {
            jedis = jedisPool.getResource();
            result = jedis.exists(key);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = false;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }
    public Long zremrangeByRank(String key, long start, long end) {
        Jedis jedis = null;

        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zremrangeByRank(key, start, end);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 如果key不存在，把key的值设置为value; "SET if Not eXists"
     * @param key 键
     * @param value 值
     * @param seconds 锁定时间（超过锁定时间后锁自动删除）
     * @return 设置成功返回1，设置失败返回0，操作失败-1
     */
    public Long setnx(String key, String value, int seconds){
        Jedis jedis = null;
        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            synchronized (this){
                result = jedis.setnx(key, value);
                jedis.expire(key,seconds);
            }
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 取有序集合的并集
     * @param dstkey
     * @param sets
     * @return
     */
    public Long zunionstore(String dstkey, String... sets){
        Jedis jedis = null;

        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zunionstore(dstkey, sets);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }
    /**
     * 取有序集合的并集
     * @param dstkey
     * @param params
     * @param sets
     * @return
     */
    public Long zunionstore(String dstkey, ZParams params, String... sets){
        Jedis jedis = null;

        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.zunionstore(dstkey, params, sets);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 设置有效时间
     * @param key 键
     * @param seconds key的有效时间
     * @return 设置成功返回1，设置失败返回0，操作失败-1
     */
    public Long expire(String key, int seconds){
        Jedis jedis = null;
        Long result = -1L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.expire(key,seconds);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = -1L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    /**
     * 缓存hash数据
     * @param key hash的key
     * @param hash hash的内容
     * @return
     */
    public String hmset(String key, Map<String, String> hash){
        Jedis jedis = null;
        String result = "";
        try {
            jedis = jedisPool.getResource();
            result = jedis.hmset(key, hash);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return result;
    }

    /**
     * 取出缓存hash的数据
     * @param key hash的key
     * @return
     */
    public Map<String, String> hgetall(String key){
        JedisPool jedisPool = this.getSlavePool();
        Jedis jedis = null;
        Map<String, String> hash = null;
        try {
            jedis = jedisPool.getResource();
            hash = jedis.hgetAll(key);
            return hash;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return hash;
    }

    /**
     * 清除所有存在的redis数据库缓存(慎重使用)
     * @return
     */
    public String flushAll(){

        Jedis jedis = null;
        String result = "";
        try {
            jedis = jedisPool.getResource();
            result = jedis.flushAll();
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return result;
    }

    /**
     * 清除当前选择的redis数据库缓存(慎重使用)
     * @return
     */
    public String flushDB(){
        Jedis jedis = null;
        String result = "";
        try {
            jedis = jedisPool.getResource();
            result = jedis.flushDB();
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }
        return result;
    }

    /**
     * 获取key剩余有效时间(单位秒)
     * @param key
     * @return key剩余有效时间
     * 在redis2.6（含）之前的版本，如果key不存在或者key未设置超期时间，则返回-1；
     * 在redis2.8（含）以后的版本，如果key未设置超期时间，则返回-1，如果key不存在则返回-2；
     */
    public Long ttl(String key){
        JedisPool jedisPool = this.getSlavePool();
        Jedis jedis = null;
        Long result = 0L;
        try {
            jedis = jedisPool.getResource();
            result = jedis.ttl(key);
            return result;
        } catch (RuntimeException e) {
            System.out.println(e);
            if (jedis != null) {
                jedisPool.returnBrokenResource(jedis);
            }

            result = 0L;
        } finally {
            if (jedis != null) {
                jedisPool.returnResource(jedis);
            }

        }

        return result;
    }

    public static void main(String[] args) {
        RedisPoolClient client = RedisPoolClient.getInstance();
        client.setEnableReadWriteSeparation(true);
        System.out.println(client.get("test"));
        for(int i = 0; i < 1000; i++){
            client.set("test",i+"");
        }

        for(int i = 0; i < 1000; i++){
            System.out.println(client.get("test"));
        }

        for(int i = 0; i < 1000; i++){
            client.set("test",i+"");
        }
    }
}
