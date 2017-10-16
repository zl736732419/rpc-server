package com.zheng.rpc.handler;

import com.zheng.rpc.common.params.RpcRequest;
import com.zheng.rpc.common.params.RpcResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

/**
 * 处理客户端请求服务接口业务逻辑
 * Created by zhenglian on 2017/10/16.
 */
public class RpcServiceHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private Map<String, Object> serviceMap;
    
    public RpcServiceHandler(Map<String, Object> serviceMap) {
        this.serviceMap = serviceMap;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) throws Exception {
        RpcResponse response = new RpcResponse();
        response.setRequestId(request.getRequestId());
        try {
            Object result = handleRequest(request);
            ctx.writeAndFlush(result);
        } catch (Exception e) {
            response.setError(e);
        }
    }

    /**
     * 处理远程客户端请求，并返回结果
     * @param request
     * @return
     */
    private Object handleRequest(RpcRequest request) throws Exception {
        if (!Optional.ofNullable(request).isPresent()) {
            throw new RuntimeException("客户端请求参数不能为空");
        }
        
        // 这里约定俗成，service名称为驼峰标识，首字母小写
        String className = request.getClassName();
        int firstChar = className.charAt(0);
        if (firstChar < 97) { // 首字母大写
            className = new StringBuilder(className.substring(0, 1).toLowerCase())
                    .append(className.substring(1)).toString();
        }
        Object targetService = serviceMap.get(className);
        Class<?> targetClass = targetService.getClass();
        Method method = targetClass.getMethod(request.getMethodName(), request.getParameterTypes());
        Object result = method.invoke(targetService, request.getParameters());
        return result;
    }
}
