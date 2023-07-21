package org.ldbcoucil.snb.impls.workloads.graphscope;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import java.io.IOException;
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ldbcoucil.snb.impls.workloads.graphscope.util.HttpConfig;

import org.ldbcouncil.snb.driver.DbConnectionState;
import org.ldbcouncil.snb.driver.DbException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.internal.Util;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

class ResponseFuture implements Callback {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    public ResponseFuture() {
    }

    @Override
    public void onFailure(Call call, IOException e) {
        e.printStackTrace();
        future.completeExceptionally(e);
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
        future.complete(response);
    }
}

public class GraphScopeDbConnectionState extends DbConnectionState {
    final private String uri;
    final private String updateUri;
    private static OkHttpClient client = null;
    public GraphScopeDbConnectionState(HttpConfig config){
        client = new OkHttpClient.Builder()
                    .dispatcher(new Dispatcher(new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                            60L, TimeUnit.SECONDS,
                            new SynchronousQueue<>(),
                            Util.threadFactory("OkHttp Dispatcher", false)
                    )))
                    .connectionPool(new ConnectionPool(config.getConnectPoolMaxIdle(),
                            config.getKeepAliveDuration(),
                            TimeUnit.MILLISECONDS))
                    .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
                    .connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS)
                    .build();
        client.dispatcher().setMaxRequests(config.getMaxRequests());
        client.dispatcher().setMaxRequestsPerHost(config.getMaxRequestsPerHost());
        uri = config.getServerAddr() + "/interactive/query";
        updateUri = config.getServerAddr() + "/interactive/update";
    }

    public byte[] syncPost(byte[] parameters) throws IOException {
        RequestBody body = RequestBody.create(parameters);
        Request request = new Request.Builder()
                .url(uri)
                .post(body)
                .build();

	try (Response response = client.newCall(request).execute()) {
	  if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
	  }
	  return response.body().bytes();
	}
    }

    public void syncPostWithoutReply(byte[] parameters) throws IOException {
        RequestBody body = RequestBody.create(parameters);
        Request request = new Request.Builder()
                .url(updateUri)
                .post(body)
                .build();
	
	try (Response response = client.newCall(request).execute()) {
	  if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
	  }
	}
    }

    @Override
    public void close() throws IOException {

    }
}
