package main.java.com.bag.evaluations;

import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.nio.*;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import main.java.com.bag.server.nettyhandlers.ClientHandler;
import main.java.com.bag.util.Log;

import java.util.concurrent.ThreadFactory;

/**
 * Netty thread for client communication.
 */
public class NettyThread extends ChannelInboundHandlerAdapter implements Runnable
{
    private static Channel       activeComChannel;
    private        String        host;
    private        int           hostPort;
    private final EventLoopGroup connectGroup;

    public NettyThread(String host, int hostPort)
    {
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        connectGroup = new NioEventLoopGroup(1,
                connectFactory, NioUdtProvider.BYTE_PROVIDER);

        this.host = host;
        this.hostPort = hostPort;
    }

    public NettyThread()
    {
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        connectGroup = new NioEventLoopGroup(1,
                connectFactory, NioUdtProvider.BYTE_PROVIDER);
    }

    /**
     * Send a message to the server.
     * @param message the byte array to send.
     */
    public synchronized void sendMessage(byte[] message)
    {
        if (activeComChannel == null)
        {
            activeComChannel = new NioSocketChannel();
            connectGroup.register(activeComChannel);
            Log.getLogger().info("Sending message: " + message.length);
        }

        activeComChannel.writeAndFlush(message);
    }

    @Override
    public void run()
    {
        try
        {
            final Bootstrap boot = new Bootstrap();
            boot.group(connectGroup)
                    .channelFactory(NioUdtProvider.BYTE_CONNECTOR)
                    .handler(new ChannelInitializer<UdtChannel>()
                    {
                        @Override
                        public void initChannel(final UdtChannel ch)
                                throws Exception
                        {
                            ch.pipeline().addLast(
                                    new LoggingHandler(LogLevel.INFO),
                                    new ClientHandler());
                        }
                    });
            // Start the client.
            final ChannelFuture f = boot.connect(host, hostPort).sync();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            // Shut down the event loop to terminate all threads.
            connectGroup.shutdownGracefully();
        }
    }
}
