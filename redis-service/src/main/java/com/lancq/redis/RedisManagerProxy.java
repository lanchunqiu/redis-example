package com.lancq.redis;

import com.lancq.redis.core.MyJedisInfo;
import com.lancq.redis.core.MyJedisPool;
import com.lancq.redis.utils.PropertiesLoader;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author lancq
 * @Description 代理类
 * @Date 2018/9/16
 **/
public class RedisManagerProxy implements MethodInterceptor {
    Logger logger = LoggerFactory.getLogger(RedisManagerProxy.class);
    public static final String REDIS_IP = "redis.ip";
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

    private MyJedisPool jedisPool;

    private static final ThreadLocal<Jedis> currJedisTheadLocal = new ThreadLocal<Jedis>();
    private Properties properties = null;
    private String ips;
    public RedisManagerProxy(){
        this.loadResources();
        JedisPoolConfig jedisPoolConfig = this.loadPoolConfig();
        System.out.println("jedisPoolConfig:" + jedisPoolConfig);
        if(ips == null){
            if(properties != null && properties.getProperty(REDIS_IP) != null){
                ips = properties.getProperty(REDIS_IP);
            } else {
                ips = RedisManager.DEFAULTIPFORMAT;
            }
        }
        logger.info(ips);
        List<MyJedisInfo> list = new ArrayList<MyJedisInfo>();
        try{
            String[] ip = ips.split(",");
            for(int i = 0; i < ip.length; i++){
                String[] ipinfo = ip[i].split(":");
                if(ipinfo.length == 2){
                    MyJedisInfo jedisInfo = new MyJedisInfo(ipinfo[0],Integer.valueOf(ipinfo[1]));
                    if(i==0){
                        jedisInfo.setMaster(true);
                    }
                    list.add(jedisInfo);
                }
            }
        } catch (Exception e){
            logger.error("ip格式不对，示例:"+RedisManager.DEFAULTIPFORMAT);
        }
        if(list.size() == 0){
            logger.error("ip格式不对，示例:"+RedisManager.DEFAULTIPFORMAT);
        }
        jedisPool = new MyJedisPool(jedisPoolConfig, list);
    }


    public void loadResources(){
        try {
            properties = PropertiesLoader.getProperties("redis.properties");
        } catch (IOException e) {
            System.out.println("=========取redis.properties文件异常:" + e.toString());
        }
    }

    public JedisPoolConfig loadPoolConfig(){
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
        return jedisPoolConfig;
    }

    public void gcJedis(Jedis jedis) {
        jedisPool.returnResource(jedis);
    }
    public Jedis createJedis(){
        return jedisPool.getResource();
    }
    public Jedis getCurrJedis(){
        return currJedisTheadLocal.get();
    }

    //被代理对象
    private RedisManager target;

    //动态生成一个新的类，使用父类的无参构造方法创建一个指定了特定回调的代理实例
    public Object getProxy(Object target){
        this.target = (RedisManager) target;

        //增强器，动态代码生成器
        Enhancer enhancer = new Enhancer();
        //设置生成类的父类类型
        enhancer.setSuperclass(target.getClass());
        //回调方法
        enhancer.setCallback(this);
        //动态生成字节码并返回代理对象
        return enhancer.create();

    }

    //拦截方法
    @Override
    public Object intercept(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
        try{
            if(this.target.getCop().get() == OP.READ){
                currJedisTheadLocal.set(jedisPool.getSlaveResource());
                logger.info("now get read jedis from:" + currJedisTheadLocal.get());
            } else {
                currJedisTheadLocal.set(jedisPool.getResource());
                logger.info("now get jedis from:"+currJedisTheadLocal.get());
            }
            Object oj = methodProxy.invoke(target, args);
            return oj;
        } catch (Throwable e){
            if(e instanceof InvocationTargetException){
                e = ((InvocationTargetException) e).getTargetException();
            }
            logger.error(e.getMessage(), e);
        } finally {
            if(this.target.getCop().get() == OP.READ){
                jedisPool.returnSlaveResource(currJedisTheadLocal.get());
            } else {
                jedisPool.returnResource(currJedisTheadLocal.get());
            }
            currJedisTheadLocal.remove();
        }
        return null;
    }

    public MyJedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(MyJedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }
}
