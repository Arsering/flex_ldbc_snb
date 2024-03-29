package org.ldbcoucil.snb.impls.workloads.graphscope.util;

public class HttpConfig {
    private String serverAddr;
    private int readTimeout;
    private int connectTimeout;
    private int connectPoolMaxIdle;
    private int keepAliveDuration;
    private int maxRequestsPerHost;
    private int maxRequests;

    public HttpConfig() {
        this.serverAddr = "http://127.0.0.1:5001/";
        this.readTimeout = 5000000;
        this.connectTimeout = 5000000;
        this.connectPoolMaxIdle = 128;
        this.keepAliveDuration = 5000;
        this.maxRequestsPerHost = 180;
        this.maxRequests = 180;
    }

    public String getServerAddr() {
        return this.serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    public int getReadTimeout() {
        return this.readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getConnectTimeout() {
        return this.connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getConnectPoolMaxIdle() {
        return this.connectPoolMaxIdle;
    }

    public void setConnectPoolMaxIdle(int connectPoolMaxIdle) {
        this.connectPoolMaxIdle = connectPoolMaxIdle;
    }

    public int getKeepAliveDuration() {
        return this.keepAliveDuration;
    }

    public void setKeepAliveDuration(int keepAliveDuration) {
        this.keepAliveDuration = keepAliveDuration;
    }

    public int getMaxRequestsPerHost() {
        return this.maxRequestsPerHost;
    }

    public void setMaxRequestsPerHost(int maxRequestsPerHost) {
        this.maxRequestsPerHost = maxRequestsPerHost;
    }

    public int getMaxRequests() {
        return this.maxRequests;
    }

    public void setMaxRequests(int maxRequests) {
        this.maxRequests = maxRequests;
    }

}
