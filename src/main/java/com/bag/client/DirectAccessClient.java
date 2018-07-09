package com.bag.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LoggingHandler;
import com.bag.operations.CreateOperation;
import com.bag.operations.DeleteOperation;
import com.bag.operations.IOperation;
import com.bag.operations.UpdateOperation;
import com.bag.reconfiguration.sensors.LoadSensor;
import com.bag.server.nettyhandlers.BAGMessageDecoder;
import com.bag.server.nettyhandlers.BAGMessageEncoder;
import com.bag.server.nettyhandlers.ClientHandler;
import com.bag.util.Constants;
import com.bag.util.Log;
import com.bag.util.storage.NodeStorage;
import com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.bag.util.Constants.COMMIT;
import static com.bag.util.Constants.WRITE_REQUEST;

/**
 * Class used to simulate a client acessing the database directly.
 */
public class DirectAccessClient implements BAGClient
{
    /**
     * The EventLoopGroup to connect.
     */
    private final EventLoopGroup connectGroup;

    /**
     * The client handler.
     */
    private final ClientHandler handler;

    /**
     * The host.
     */
    private final String host;

    /**
     * The port of the host.
     */
    private final int hostPort;

    /**
     * The kryo pool.
     */
    private final KryoPool kryoPool;

    /**
     * Create a threadsafe version of kryo.
     */
    private final KryoFactory factory = () ->
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

    public DirectAccessClient(final String host, final int hostPort)
    {
        kryoPool = new KryoPool.Builder(factory).softReferences().build();

        this.host = host;
        this.hostPort = hostPort;
        handler = new ClientHandler();
        Log.getLogger().warn("Setting up connecting with host: " + host + " at port: " + hostPort);

        connectGroup = new NioEventLoopGroup();
        try
        {
            final Bootstrap boot = new Bootstrap();
            boot.group(connectGroup)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(new InetSocketAddress(this.host, this.hostPort))
                    .handler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(final SocketChannel ch) throws Exception
                        {
                            ch.pipeline().addLast(
                                    new BAGMessageEncoder(),
                                    new BAGMessageDecoder(),
                                    new LoggingHandler(Log.BAG_DESC),
                                    handler);
                        }
                    });
            boot.connect().sync();
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Error instantiating DirectAccessClient", e);
        }

        Log.getLogger().setLevel(Level.WARN);
    }

    @Override
    public BlockingQueue<Object> getReadQueue()
    {
        return handler.getReadQueue();
    }

    /**
     * write requests. (Only reach database on commit)
     */
    @Override
    public void write(final Object identifier, final Object value)
    {
        Log.getLogger().info("Sending write!");
        if (identifier == null && value == null)
        {
            Log.getLogger().error("Unsupported write operation");
            return;
        }

        final IOperation operation;
        //Must be a create request.
        if (identifier == null)
        {
            operation = new CreateOperation<>((Serializable) value);
        }
        else if (value == null)
        {
            operation = new DeleteOperation<>((Serializable) identifier);
        }
        else
        {
            operation = new UpdateOperation((Serializable) identifier, (Serializable) value);
        }

        final List<IOperation> toSend = new ArrayList<>();
        toSend.add(operation);

        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, getID());
        kryo.writeObject(output, toSend);

        handler.sendMessage(output.getBuffer());

        Log.getLogger().info("Finishing write!");
        output.close();
        kryoPool.release(kryo);
    }

    @Override
    public void read(final Object... identifiers)
    {
        Log.getLogger().info("Sending read!");
        final List<Object> list = new ArrayList<>();

        for (final Object item : identifiers)
        {
            if (item instanceof NodeStorage || item instanceof RelationshipStorage)
            {
                list.add(item);
            }
            else
            {
                Log.getLogger().info("Invalid type to read " + item.getClass().getName());
            }
        }


        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, getID());
        kryo.writeObject(output, list);


        handler.sendMessage(output.getBuffer());
        Log.getLogger().info("Finishing read!");
        output.close();
        kryoPool.release(kryo);
    }

    @Override
    public void commit()
    {
        Log.getLogger().info("Sending commit!");
        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 1024000);
        kryo.writeObject(output, getID());
        kryo.writeObject(output, new ArrayList<>());
        handler.sendMessage(output.getBuffer());
        output.close();
        kryoPool.release(kryo);
        Log.getLogger().info("Finishing commit!");
    }

    @Override
    public boolean isCommitting()
    {
        return false;
    }

    @Override
    public int getID()
    {
        return 1;
    }

    @Override
    public boolean hasRead()
    {
        return true;
    }

    @Override
    public void turnUpWrites()
    {
        /*
         * Do nothing.d
         */
    }
}