package com.zheng.rpc.server;

import com.zheng.rpc.annotation.RpcService;
import com.zheng.rpc.common.encrypt.RpcDecoder;
import com.zheng.rpc.common.encrypt.RpcEncoder;
import com.zheng.rpc.common.params.RpcRequest;
import com.zheng.rpc.common.params.RpcResponse;
import com.zheng.rpc.handler.RpcServiceHandler;
import com.zheng.rpc.registry.ServiceRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 用于将带有@RpcService注解的服务暴露出来
 * 供rpc客户端远程调用
 * 1. 需要将服务器地址注册到zookeeper中
 * 2. 需要开启rpc服务器将服务暴露出来
 * Created by zhenglian on 2017/10/15.
 */
public class RpcServer implements ApplicationContextAware, InitializingBean {
    /**
     * 服务所在服务器地址
     */
    private String serverAddress;
    /**
     * 服务器注册中心
     */
    private ServiceRegistry serviceRegistry;

    
    /**
     * 用于存储@RpcService注册的服务
     */
    private Map<String, Object> serviceMap = new HashMap<>();
    
    
    public RpcServer(String serverAddress, ServiceRegistry serviceRegistry) {
        this.serverAddress = serverAddress;
        this.serviceRegistry = serviceRegistry;
    }


    /**
     * 采用netty暴露服务
     * 接受并处理远程客户端服务调用请求
     * 暴露服务器地址
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 配置netty
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            /*
                                服务端业务逻辑
                                1. 解码客户端发送过来的请求 in1
                                2. 处理业务逻辑 in2
                                3. 返回响应信息 out
                             */
                            socketChannel.pipeline()
                                    .addLast(new RpcDecoder(RpcRequest.class))
                                    .addLast(new RpcEncoder(RpcResponse.class))
                                    .addLast(new RpcServiceHandler(serviceMap));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            // 启动netty服务器
            String[] array = serverAddress.split(":");
            String host = array[0];
            Integer port = Integer.parseInt(array[1]);
            ChannelFuture future = bootstrap.bind(host, port).sync(); // 同步阻塞直到服务端绑定端口监听
            // 将服务器注册到zookeeper上
            if (Optional.ofNullable(serviceRegistry).isPresent()) {
                serviceRegistry.register(serverAddress);
            }
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    /**
     * 通过ctx将@RpcService注解的服务获取到并放入到serviceMap
     * @param ctx
     * @throws BeansException
     */
    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        Map<String, Object> serviceBeans = ctx.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isEmpty(serviceBeans)) {
            return;
        }
        
        serviceBeans.values().stream()
                .filter(bean -> Optional.ofNullable(bean).isPresent())
                .forEach(bean -> {
                    String serviceName = bean.getClass().getAnnotation(RpcService.class).value();
                    serviceMap.put(serviceName, bean);
                });
    }
}
