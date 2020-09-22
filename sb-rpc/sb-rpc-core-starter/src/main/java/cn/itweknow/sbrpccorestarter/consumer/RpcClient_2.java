package cn.itweknow.sbrpccorestarter.consumer;


import cn.itweknow.sbrpccorestarter.common.RpcDecoder;
import cn.itweknow.sbrpccorestarter.common.RpcEncoder;
import cn.itweknow.sbrpccorestarter.model.RpcRequest;
import cn.itweknow.sbrpccorestarter.model.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author 无用代码
 * @date 2018/10/26 18:09
 * @description
 */
public class RpcClient_2 extends SimpleChannelInboundHandler<RpcResponse> {

    private Logger logger = LoggerFactory.getLogger(RpcClient_2.class);

    private String host;

    private int port;

    EventLoopGroup workerGroup;
    ChannelFuture channelFuture;
    Map<String,Channel> channelMap = new HashMap<>();
    private CompletableFuture<String> future;

    /**
     * 用来接收服务器端的返回的。
     */
    private RpcResponse response;

    public RpcClient_2(String host, int port) {
        this.host = host;
        this.port = port;
        connect();
    }

    void connect(){
        if (this.workerGroup != null){
            return;
        }
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline()
                                    .addLast(new RpcEncoder(RpcRequest.class))
                                    .addLast(new RpcDecoder(RpcResponse.class))
                                    .addLast(RpcClient_2.this);
                        }
                    });
            // 连接服务器
            logger.info("RpcStarter::connect provider >> {}:{}", host,port);
            channelFuture = bootstrap.connect(host, port).sync();
            channelMap.put(host+""+port,channelFuture.channel());
            future = new CompletableFuture<>();
            this.workerGroup = workerGroup;
        } catch (Exception e) {
            logger.error("RpcStarter::client send msg error,", e);
        } finally {
//            workerGroup.shutdownGracefully();
        }
    }

    public RpcResponse send(RpcRequest request){
        try {
            channelFuture.channel().writeAndFlush(request).sync();
            future.get();
            if (response != null) {
                // 关闭netty连接。
                channelFuture.channel().closeFuture().sync();
            }
            return response;
        } catch (Exception e) {
            logger.error("RpcStarter::client send msg error,", e);
            return null;
        } finally {

        }
    }

    void close(){
        workerGroup.shutdownGracefully();
    }

//    public RpcResponse send(RpcRequest request){
//        EventLoopGroup workerGroup = new NioEventLoopGroup();
//        try {
//            Bootstrap bootstrap = new Bootstrap();
//            bootstrap.group(workerGroup)
//                    .option(ChannelOption.SO_KEEPALIVE, true)
//                    .channel(NioSocketChannel.class)
//                    .handler(new ChannelInitializer<SocketChannel>() {
//                        @Override
//                        protected void initChannel(SocketChannel socketChannel) throws Exception {
//                            socketChannel.pipeline()
//                                    .addLast(new RpcEncoder(RpcRequest.class))
//                                    .addLast(new RpcDecoder(RpcResponse.class))
//                                    .addLast(RpcClient.this);
//                        }
//                    });
//            // 连接服务器
//            logger.info("RpcStarter::connect provider >> {}:{}", host,port);
//            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
//            channelFuture.channel().writeAndFlush(request).sync();
//            future = new CompletableFuture<>();
//            future.get();
//            if (response != null) {
//                // 关闭netty连接。
//                channelFuture.channel().closeFuture().sync();
//            }
//            return response;
//        } catch (Exception e) {
//            logger.error("RpcStarter::client send msg error,", e);
//            return null;
//        } finally {
//            workerGroup.shutdownGracefully();
//        }
//    }


    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext,
                                RpcResponse rpcResponse) throws Exception {
        logger.info("RpcStarter::client get request result,{}", rpcResponse);
        this.response = rpcResponse;
        future.complete("");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("RpcStarter::netty client caught exception,", cause);
        ctx.close();
    }
}
