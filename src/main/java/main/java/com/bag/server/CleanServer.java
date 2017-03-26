package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * Server used to communicate with graph databases directly without the use of BAG.
 */
public class CleanServer extends ChannelInboundHandlerAdapter
{
    /**
     * Create a threadsafe version of kryo.
     */
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

    /**
     * The database access for this class.
     */
    private final IDatabaseAccess access;

    /**
     * The instance of the database access.
     */
    private final String instance;

    /**
     * Amount of succesful executions.
     */
    private int numberOfExecutions = 0;

    /**
     * Create an instance of this server.
     * @param instance the instance of the db.
     */
    private CleanServer(final String instance)
    {
        this.instance = instance;
        access = ServerWrapper.instantiateDBAccess(instance, 0);
        if (access == null)
        {
            throw new UnsupportedOperationException("Wrong instance for this server");
        }
        access.start();
    }

    /**
     * Terminate the database access.
     */
    private void terminate()
    {
        access.terminate();
    }

    /**
     * Get the number of executions.
     * @return the number.
     */
    private int getNumberOfExecutions()
    {
        return numberOfExecutions;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        // Discard the received data silently.
        ((ByteBuf) msg).release(); // (3)

        final Input input = new Input(((ByteBuf) msg).array());

        List returnValue = kryo.readObject(input, ArrayList.class);
        Log.getLogger().info("Received message!");
        for(Object obj: returnValue)
        {
            if (obj instanceof Operation)
            {
                ((Operation) obj).apply(access, 0);
                numberOfExecutions++;
            }
            else if(obj instanceof NodeStorage)
            {
                try
                {
                    access.readObject(obj,0);
                    numberOfExecutions++;
                }
                catch (OutDatedDataException e)
                {
                    Log.getLogger().info("Unable to retrieve data at clean server with instance: " + instance, e);
                }
            }
        }

        ((ByteBuf) msg).release();
    }

    /**
     * Main method to start the clean server.
     * @param args the arguments to start it with.
     */
    public static void main(String[] args)
    {
        if (args.length < 3)
        {
            return;
        }

        final int serverPort = Integer.parseInt(args[0]);
        final String tempInstance = args[2];

        final String instance;

        if (tempInstance.toLowerCase().contains("titan"))
        {
            instance = Constants.TITAN;
        }
        else if (tempInstance.toLowerCase().contains("orientdb"))
        {
            instance = Constants.ORIENTDB;
        }
        else if (tempInstance.toLowerCase().contains("sparksee"))
        {
            instance = Constants.SPARKSEE;
        }
        else
        {
            instance = Constants.NEO4J;
        }

       

        final CleanServer server = new CleanServer(instance);


        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try
        {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer()
                    {
                        @Override
                        protected void initChannel(final Channel ch) throws Exception
                        {
                            ch.pipeline().addLast(server);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(serverPort).sync(); // (7)

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException e)
        {
            Log.getLogger().info("Problem with execution of netty.", e);
            Thread.currentThread().interrupt();
        }
        finally
        {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

        server.terminate();

        Log.getLogger().info("Executed operations: " + server.getNumberOfExecutions());

    }
}
