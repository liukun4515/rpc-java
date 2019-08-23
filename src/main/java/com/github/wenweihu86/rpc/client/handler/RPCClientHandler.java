package com.github.wenweihu86.rpc.client.handler;

import com.github.wenweihu86.rpc.client.RPCClient;
import com.github.wenweihu86.rpc.protocol.ProtocolProcessor;
import com.github.wenweihu86.rpc.protocol.standard.StandardProtocol;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RPCClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(RPCClientHandler.class);

    private RPCClient rpcClient;

    public RPCClientHandler(RPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    // client handler的任务就是
    // 处理请求和以及对应的client
    public void channelRead0(ChannelHandlerContext ctx, Object fullResponse) throws Exception {
        ProtocolProcessor protocol = StandardProtocol.instance();
        // client获得response的内容进行处理
        protocol.processResponse(rpcClient, fullResponse);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error(cause.getMessage(), cause);
        ctx.close();
    }

}
