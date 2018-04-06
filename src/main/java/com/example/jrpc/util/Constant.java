package com.example.jrpc.util;

import java.util.concurrent.TimeUnit;

/**
 * Created by yilong on 2018/1/25.
 */
public class Constant {
    public static final String SYSTEM_NAME = "SSTableFile";
    public static final long MEM_TABLE_NON_LOCK_SIZE_THRESHOLD = 100;

    public static final long MEM_TABLE_FLUSH_INTO_FILE_KEEP_TIME = 30*1000;

    public static final int LEVEL_0 = 0;
    public static final int LEVEL_1 = 1;
    public static final int LEVEL_2 = 2;

    public static final int TRY_Time = 10;
    public static final TimeUnit TRY_TimeUnit = TimeUnit.MILLISECONDS;

    public static final int KV_LEN_SIZE = 4;
    public static final int BLOCK_SIZE = 128; //1024 * 1024;

    public static final int LONG_SIZE = 8;

}
