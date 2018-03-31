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
import main.java.com.bag.evaluations.NettyClient;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.nettyhandlers.BAGMessageDecoder;
import main.java.com.bag.server.nettyhandlers.BAGMessageEncoder;
import main.java.com.bag.server.nettyhandlers.ClientHandler;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Class used to simulate a client acessing the database directly.
 */
public class DirectAccessClient implements BAGClient
{

    private NettyClient           server;
    private BlockingQueue<Object> readQueue;
    private final EventLoopGroup  connectGroup;
    private ClientHandler         handler;
    private String                host;
    private int                   hostPort;
    private KryoPool              kryoPool;
    private ArrayList<IOperation> writeSet;

    /**
     * Create a threadsafe version of kryo.
     */
    public KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        return kryo;
    };


    public DirectAccessClient(String host, int hostPort) {
        kryoPool = new KryoPool.Builder(factory).softReferences().build();
        writeSet = new ArrayList<>();
        this.readQueue = new ArrayBlockingQueue<>(500);
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        connectGroup = new NioEventLoopGroup(1,
                connectFactory, NioUdtProvider.BYTE_PROVIDER);

        this.host = host;
        this.hostPort = hostPort;

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
                                    new BAGMessageEncoder(),
                                    new BAGMessageDecoder(),
                                    new LoggingHandler(Log.BAG_DESC),
                                    handler);
                        }
                    });
            // Start the client.
            boot.connect(this.host, this.hostPort).sync();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public BlockingQueue<Object> getReadQueue() {
        return handler.getReadQueue();
    }

    /**
     * write requests. (Only reach database on commit)
     */
    @Override
    public void write(final Object identifier, final Object value)
    {
        if(identifier == null && value == null)
        {
            Log.getLogger().error("Unsupported write operation");
            return;
        }

        //Must be a create request.
        if(identifier == null)
        {
            handleCreateRequest(value);
            return;
        }

        //Must be a delete request.
        if(value == null)
        {
            handleDeleteRequest(identifier);
            return;
        }

        handleUpdateRequest(identifier, value);
    }

    /**
     * Fills the updateSet in the case of an update request.
     * @param identifier the value to write to.
     * @param value what should be written.
     */
    private void handleUpdateRequest(Object identifier, Object value)
    {
        //todo edit create request if equal.
        if(identifier instanceof NodeStorage && value instanceof NodeStorage)
        {
            writeSet.add(new UpdateOperation<>((NodeStorage) identifier,(NodeStorage) value));
        }
        else if(identifier instanceof RelationshipStorage && value instanceof RelationshipStorage)
        {
            writeSet.add(new UpdateOperation<>((RelationshipStorage) identifier,(RelationshipStorage) value));
        }
        else
        {
            Log.getLogger().error("Unsupported update operation can't update a node with a relationship or vice versa");
        }
    }

    /**
     * Fills the createSet in the case of a create request.
     * @param value object to fill in the createSet.
     */
    private void handleCreateRequest(Object value)
    {
        if(value instanceof NodeStorage)
        {
            writeSet.add(new CreateOperation<>((NodeStorage) value));
        }
        else if(value instanceof RelationshipStorage)
        {
            writeSet.add(new CreateOperation<>((RelationshipStorage) value));
        }
        else
        {
            Log.getLogger().error("Unsupported update operation can't update a node with a relationship or vice versa");
        }
    }

    /**
     * Fills the deleteSet in the case of a delete requests and deletes the node also from the create set and updateSet.
     * @param identifier the object to delete.
     */
    private void handleDeleteRequest(Object identifier)
    {
        //todo we can delete creates here.
        if(identifier instanceof NodeStorage)
        {
            writeSet.add(new DeleteOperation<>((NodeStorage) identifier));
        }
        else if(identifier instanceof RelationshipStorage)
        {
            writeSet.add(new DeleteOperation<>((RelationshipStorage) identifier));
        }
        else
        {
            Log.getLogger().error("Unsupported update operation can't update a node with a relationship or vice versa");
        }
    }

    @Override
    public void read(Object... identifiers) {
        List<Object> list = new ArrayList<>();

        for (Object item : identifiers)
        {
            if (item instanceof NodeStorage || item instanceof RelationshipStorage)
                list.add(item);
            else
                Log.getLogger().error("Invalid type to read " + item.getClass().getName());
        }
        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, list);

        if (Log.getLogger().getLevel() == Level.INFO) {
            for (Object item : list)
                Log.getLogger().info("Reading: " + item.toString());
        }

        handler.sendMessage(output.getBuffer());
        output.close();
        kryoPool.release(kryo);
    }

    @Override
    public void commit() {
        final Kryo kryo = kryoPool.borrow();
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, writeSet);


        if (Log.getLogger().getLevel() == Level.INFO) {
            for (Object item : writeSet)
                Log.getLogger().info("Commiting: " + item.toString());

            if (writeSet.size() == 0)
                Log.getLogger().info("Commiting empty writeSet");
        }

        handler.sendMessage(output.getBuffer());
        output.close();
        kryoPool.release(kryo);
        writeSet.clear();
        try {
            while (getReadQueue().take() != TestClient.FINISHED_READING);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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