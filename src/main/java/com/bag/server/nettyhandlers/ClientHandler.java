package com.bag.server.nettyhandlers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import com.bag.client.TestClient;
import com.bag.operations.CreateOperation;
import com.bag.operations.DeleteOperation;
import com.bag.operations.UpdateOperation;
import com.bag.reconfiguration.sensors.LoadSensor;
import com.bag.util.Log;
import com.bag.util.storage.NodeStorage;
import com.bag.util.storage.RelationshipStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handler implementation for the echo client. It initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server on activation.
 */
public class ClientHandler extends SimpleChannelInboundHandler<BAGMessage>
{
    private static final KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        kryo.register(LoadSensor.LoadDesc.class, 400);
        return kryo;
    };

    /**
     * Message context.
     */
    private ChannelHandlerContext ctx;

    /**
     * Lock object to let the thread wait for a read return.
     */
    private final BlockingQueue<Object> readQueue = new LinkedBlockingQueue<>();

    /**
     * The last object in read queue.
     */
    public static final Object FINISHED_READING = new Object();

    public ClientHandler()
    {
        super(false);
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
    public void channelRead0(final ChannelHandlerContext ctx, final BAGMessage msg)
    {
        Log.getLogger().info("Received response");
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        final Input input = new Input(msg.buffer);
        final List returnValue = kryo.readObject(input, ArrayList.class);
        input.close();
        pool.release(kryo);

        for (final Object item : returnValue)
        {
            if (item instanceof DeleteOperation)
            {
                Log.getLogger().info("Finished handler adding finished to queue.");
                readQueue.add(TestClient.FINISHED_READING);
            }
            else
            {
                Log.getLogger().info("Received: " + item.toString());
                readQueue.add(item);
            }
        }
    }

    /**
     * Send the message.
     * @param bytes the bytes to send.
     */
    public void sendMessage(final byte[] bytes)
    {
        try
        {
            final BAGMessage message = new BAGMessage();
            message.buffer = bytes;
            message.size = bytes.length;
            this.ctx.writeAndFlush(message).sync();
        }
        catch (final InterruptedException e)
        {
            Log.getLogger().warn("Error sending message", e);
        }
    }
}
