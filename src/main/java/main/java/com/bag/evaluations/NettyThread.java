package main.java.com.bag.evaluations;

import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.nio.*;

/**
 * Netty thread for client communication.
 */
public class NettyThread extends ChannelInboundHandlerAdapter implements Runnable
{
    private static Channel activeComChannel;
    private        String  host;
    private        int     hostPort;

    public NettyThread(String host, int hostPort)
    {
        this.host = host;
        this.hostPort = hostPort;
    }

    /**
     * Send a message to the server.
     * @param message the byte array to send.
     */
    public void sendMessage(byte[] message)
    {
        if (activeComChannel == null)
        {
            activeComChannel = new NioSocketChannel();
        }

        activeComChannel.writeAndFlush(message);
    }

    @Override
    public void run()
    {
        EventLoopGroup nioGroup = new NioEventLoopGroup();

        Bootstrap b = new Bootstrap();
        b.group(nioGroup)
                .channel(NioSocketChannel.class)
                .handler(new NettyThread(host, hostPort));

        b.connect(host, hostPort).syncUninterruptibly();
    }
}
