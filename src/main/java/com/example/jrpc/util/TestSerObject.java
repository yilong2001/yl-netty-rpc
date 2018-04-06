package com.example.jrpc.util;

/**
 * Created by yilong on 2018/3/19.
 */
public class TestSerObject implements java.io.Serializable {
    public String x,y;
    public Object z;
    public TestSerObject(String x, String y, Object z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }
}
