package main.java.com.bag.server.nettyhandlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 25   * Handler implementation for the echo client. It initiates the ping-pong
 * 26   * traffic between the echo client and server by sending the first message to
 * 27   * the server on activation.
 * 28
 */
public class ClientHandler extends SimpleChannelInboundHandler<ByteBuf>
{
    private final ByteBuf message;
    private ChannelHandlerContext ctx;

    public ClientHandler()
    {
        super(false);

        message = Unpooled.buffer(256);
        for (int i = 0; i < message.capacity(); i++)
        {
            message.writeByte((byte) i);
        }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        this.ctx = ctx;
    }

    //Handle response.
    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
    {
        ctx.write(msg);
    }

    /**
     * Send the message.
     * @param bytes the bytes to send.
     */
    public void sendMessage(byte[] bytes)
    {
        try
        {
            message.retain();
            message.resetWriterIndex();
            message.writeBytes(bytes);
            this.ctx.writeAndFlush(message).sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
