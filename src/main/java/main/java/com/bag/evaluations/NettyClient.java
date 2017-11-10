package main.java.com.bag.evaluations;

import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import main.java.com.bag.server.nettyhandlers.ClientHandler;

import java.util.concurrent.ThreadFactory;

/**
 * Netty thread for client communication.
 */
public class NettyClient
{
    private        String        host;
    private        int           hostPort;
    private final EventLoopGroup connectGroup;

    private ClientHandler handler;

    public NettyClient(String host, int hostPort)
    {
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        connectGroup = new NioEventLoopGroup(1,
                connectFactory, NioUdtProvider.BYTE_PROVIDER);

        this.host = host;
        this.hostPort = hostPort;
    }

    /**
     * Send a message to the server.
     * @param message the byte array to send.
     */
    public synchronized void sendMessage(byte[] message)
    {
        handler.sendMessage(message);
    }

    /**
     * Shut down netty.
     */
    public void shutDown()
    {
        connectGroup.shutdownGracefully();
    }

    public void runNetty()
    {
        try
        {
            final Bootstrap boot = new Bootstrap();
            handler = new ClientHandler();
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
                                    handler);
                        }
                    });
            // Start the client.
            boot.connect(host, hostPort).sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
