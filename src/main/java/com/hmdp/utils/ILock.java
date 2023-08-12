package com.hmdp.utils;

/**
 * @author dongjinmin
 * @date 2023/8/3
 * @Description
 */
public interface ILock {

    /**
     * 尝试获取锁
     * @param timeoutSec 锁持有的超时时间，过期自动释放
     * @return true 代表获取锁成功， false 代表获取锁失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unLock();
}
