package com.zheng.rpc.registry;

import com.zheng.rpc.constants.ZkConstants;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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
        createNode(data);
    }

    /**
     * 判断zookeeper集群中指定节点是否存在
     * @param nodePath
     * @return
     * @throws Exception
     */
    private boolean existNode(String nodePath) throws Exception {
        Stat stat = zk.exists(nodePath, false);
        return Optional.ofNullable(stat).isPresent();
    }

    /**
     * 创建服务器注册的父级节点
     * 父级节点是持久化类型的
     * 如果没有创建父级节点而直接创建子节点，zookeeper将报错
     */
    private void createNode(String data) {
        // 创建节点前先成功创建父节点
        createParentNode();
        // 创建子节点
        String childNode = new StringBuilder(ZkConstants.PARENT_NODE)
                .append(ZkConstants.DATA_NODE).toString();
        try {
            if (!existNode(childNode)) {
                zk.create(childNode, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
            }
        } catch (Exception e) {
            logger.error("创建节点{}发生异常，异常信息: {}", childNode, e.getLocalizedMessage());
        }
    }

    /**
     * 创建父节点
     * @throws Exception
     */
    private void createParentNode() {
        String node = ZkConstants.PARENT_NODE;
        try {
            if (!existNode(node)) {
                zk.create(node, "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            logger.error("创建节点{}发生异常，异常信息: {}", node, e.getLocalizedMessage());
        }
    }
}
