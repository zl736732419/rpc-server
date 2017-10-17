package com.zheng.rpc.registry;

import com.zheng.rpc.common.constants.ZkConstants;
import com.zheng.rpc.common.utils.ZkUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper注册中心，用于将服务器地址注册到zookeeper中
 * 注册的服务器在目标节点形如/servers/serverxxxxxxx
 * Created by zhenglian on 2017/10/15.
 */
public class ServiceRegistry {
    private Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);
    /**
     * zk地址
     */
    private String registryAddress;

    private ZooKeeper zk = null;

    /**
     * 这里是为了确保zk连接建立成功
     */
    private CountDownLatch latch = new CountDownLatch(1);
    
    public ServiceRegistry(String serviceRegistry) {
        this.registryAddress = serviceRegistry;
    }

    /**
     * 注册给定的服务器地址到zk
     * @param data 目标服务器地址
     */
    public void register(String data) {
        connectZkServer();
        if (Optional.ofNullable(zk).isPresent()) {
            saveData(data);
        }
    }

    /**
     * 连接到zk服务器
     */
    private void connectZkServer() {
        try {
            zk = new ZooKeeper(registryAddress, ZkConstants.SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    Event.KeeperState state = watchedEvent.getState();
                    if (Objects.equals(state, Event.KeeperState.SyncConnected)) {
                        latch.countDown();
                    }
                }
            });
            latch.await(); // 等待zk连接建立完毕再执行下一步
        } catch (Exception e) {
            logger.error("error: " + e.getLocalizedMessage());
        }
    }

    /**
     * 给指定服务器创建节点并保存
     * @param data
     */
    private void saveData(String data) {
        ZkUtil.createNode(zk, data);
    }
}
