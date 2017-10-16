package com.zheng.rpc.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * rpc远程服务需要添加的注解
 * 便于将本地服务暴露到远程
 * Created by zhenglian on 2017/10/15.
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = ElementType.TYPE)
@Component
public @interface RpcService {
    String value() default "";
}
