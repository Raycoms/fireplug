package main.java.com.bag.server.nettyhandlers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import main.java.com.bag.client.TestClient;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 25   * Handler implementation for the echo client. It initiates the ping-pong
 * 26   * traffic between the echo client and server by sending the first message to
 * 27   * the server on activation.
 * 28
 */
public class ClientHandler extends SimpleChannelInboundHandler<BAGMessage>
{
    private static KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        return kryo;
    };


    private ByteBufAllocator allocator;
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
        allocator = PooledByteBufAllocator.DEFAULT;
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
    public void channelRead0(ChannelHandlerContext ctx, BAGMessage msg)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        final Input input = new Input(msg.buffer);
        final List returnValue = kryo.readObject(input, ArrayList.class);
        input.close();
        pool.release(kryo);

        for (Object item : returnValue) {
            if (item instanceof DeleteOperation) {
                Log.getLogger().info("Finished Reading");
                readQueue.add(TestClient.FINISHED_READING);
            }
            else {
                if (Log.getLogger().getLevel() == Level.INFO)
                    Log.getLogger().info("Received: " + item.toString());
                readQueue.add(item);
            }
        }
    }

    /**
     * Send the message.
     * @param bytes the bytes to send.
     */
    public void sendMessage(byte[] bytes)
    {
        try
        {
            BAGMessage message = new BAGMessage();
            message.buffer = bytes;
            message.size = bytes.length;
            this.ctx.writeAndFlush(message).sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
