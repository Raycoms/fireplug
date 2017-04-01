package main.java.com.bag.server.nettyhandlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    /**
     * Lock object to let the thread wait for a read return.
     */
    private BlockingQueue<Object> readQueue = new LinkedBlockingQueue<>();

    /**
     * The last object in read queue.
     */
    public static final Object FINISHED_READING = new Object();

    public ClientHandler()
    {
        super(false);

        message = Unpooled.buffer(256);
        for (int i = 0; i < message.capacity(); i++)
        {
            message.writeByte((byte) i);
        }
    }


    /**
     * Get the blocking queue.
     * @return the queue.
     */
    public BlockingQueue<Object> getReadQueue()
    {
        return readQueue;
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
        readQueue.add(FINISHED_READING);
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

        try
        {
            readQueue.take();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
