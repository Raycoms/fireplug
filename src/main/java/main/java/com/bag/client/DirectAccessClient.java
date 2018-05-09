package main.java.com.bag.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.reconfiguration.sensors.LoadSensor;
import main.java.com.bag.server.nettyhandlers.BAGMessageDecoder;
import main.java.com.bag.server.nettyhandlers.BAGMessageEncoder;
import main.java.com.bag.server.nettyhandlers.ClientHandler;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

import static main.java.com.bag.util.Constants.COMMIT;
import static main.java.com.bag.util.Constants.WRITE_REQUEST;

/**
 * Class used to simulate a client acessing the database directly.
 */
public class DirectAccessClient implements BAGClient
{
    private final EventLoopGroup        connectGroup;
    private final ClientHandler         handler;
    private final String                host;
    private final int                   hostPort;
    private final KryoPool              kryoPool;

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
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        connectGroup = new NioEventLoopGroup(1,
                connectFactory, NioUdtProvider.BYTE_PROVIDER);

        this.host = host;
        this.hostPort = hostPort;
        handler = new ClientHandler();

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
                                    new BAGMessageEncoder(),
                                    new BAGMessageDecoder(),
                                    new LoggingHandler(Log.BAG_DESC),
                                    handler);
                        }
                    });
            // Start the client.
            boot.connect(this.host, this.hostPort).sync();
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Error instantiating DirectAccessClient", e);
        }
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
        Log.getLogger().warn("Sending write!");
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
        kryo.writeObject(output, toSend);

        handler.sendMessage(output.getBuffer());

        Log.getLogger().warn("Finishing write!");
        output.close();
        kryoPool.release(kryo);
    }

    @Override
    public void read(final Object... identifiers)
    {
        Log.getLogger().warn("Sending read!");
        final List<Object> list = new ArrayList<>();

        for (final Object item : identifiers)
        {
            if (item instanceof NodeStorage || item instanceof RelationshipStorage)
            {
                list.add(item);
            }
            else
            {
                Log.getLogger().error("Invalid type to read " + item.getClass().getName());
            }
        }
        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, list);

        if (Log.getLogger().getLevel() == Level.INFO)
        {
            for (final Object item : list)
            {
                Log.getLogger().info("Reading: " + item.toString());
            }
        }

        handler.sendMessage(output.getBuffer());
        Log.getLogger().warn("Finishing read!");
        output.close();
        kryoPool.release(kryo);
    }

    @Override
    public void commit()
    {
        Log.getLogger().warn("Sending commit!");
        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, new ArrayList<>());

        handler.sendMessage(output.getBuffer());
        output.close();
        kryoPool.release(kryo);
        Log.getLogger().warn("Finishing commit!");
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
}