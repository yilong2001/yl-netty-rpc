package com.example.jrpc.nettyrpc.netty;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import java.lang.reflect.Field;

/**
 * Created by yilong on 2018/3/14.
 */
public class NettyUtils {
    public static PooledByteBufAllocator createPooledByteBufAllocator(
            boolean allowDirectBufs,
            boolean allowCache,
            int numCores) {
        if (numCores == 0) {
            numCores = Runtime.getRuntime().availableProcessors();
        }
        return new PooledByteBufAllocator(
                allowDirectBufs && PlatformDependent.directBufferPreferred(),
                Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores),
                Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
                getPrivateStaticField("DEFAULT_PAGE_SIZE"),
                getPrivateStaticField("DEFAULT_MAX_ORDER"),
                allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
                allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
                allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0
        );
    }

    /** Used to get defaults from Netty's private static fields. */
    private static int getPrivateStaticField(String name) {
        try {
            Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.getInt(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
