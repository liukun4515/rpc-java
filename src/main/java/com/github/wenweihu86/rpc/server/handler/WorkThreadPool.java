package com.github.wenweihu86.rpc.server.handler;

import com.github.wenweihu86.rpc.protocol.ProtocolProcessor;
import com.github.wenweihu86.rpc.protocol.standard.StandardProtocol;
import com.github.wenweihu86.rpc.utils.CustomThreadFactory;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by wenweihu86 on 2017/4/25.
 *
 * 工作的线程池
 * 1. executor java原生线程池对象
 * 2. 任务的阻塞队列
 */
public class WorkThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(WorkThreadPool.class);

    private ThreadPoolExecutor executor;
    private BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<Runnable>();

    public WorkThreadPool(int workThreadNum) {
        executor = new ThreadPoolExecutor(
                workThreadNum,
                workThreadNum,
                60L, TimeUnit.SECONDS, blockingQueue,
                new CustomThreadFactory("worker-thread"));
        executor.prestartAllCoreThreads();
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public static class WorkTask implements Runnable {
        /**
         * work task
         * 一个work 需要进行处理的内容，或者工作的内容
         */
        private Object fullRequest;
        private ChannelHandlerContext ctx;

        public WorkTask(ChannelHandlerContext ctx,
                        Object fullRequest) {
            this.fullRequest = fullRequest;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            // 协议的内容
            ProtocolProcessor protocol = StandardProtocol.instance();
            // 处理协议
            Object fullResponse = protocol.processRequest(fullRequest);
            // 获得结果，结果写入到对应的context中
            ctx.channel().writeAndFlush(fullResponse);
        }

    }

}
