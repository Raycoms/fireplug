package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;

import io.netty.channel.udt.UdtChannel;
import io.netty.channel.udt.nio.NioUdtProvider;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
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
import org.apache.log4j.Level;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * Server used to communicate with graph databases directly without the use of BAG.
 */
public class CleanServer extends SimpleChannelInboundHandler<ByteBuf>
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
     * Amount of data sent since last reset.
     */
    private int throughput;

    /**
     * Amount of aborts since last reset.
     */
    private int aborts;

    /**
     * Amount of commits since last reset.
     */
    private int committedTransactions;

    /**
     * Time of last commit.
     */
    private double lastCommit;

    /**
     * Id of the server.
     */
    private final int id;

    /**
     * Create an instance of this server.
     *
     * @param access the instance of the db.
     */
    private CleanServer(final IDatabaseAccess access, final int id)
    {
        this.access = access;
        this.id = id;
        lastCommit = System.nanoTime()/Constants.NANO_TIME_DIVIDER;
    }

    /**
     * Terminate the database access.
     */
    private void terminate()
    {
        access.terminate();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
    {
        Log.getLogger().warn("Exception caused in transfer", cause);
        ctx.close();
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        byte[] bytes = new byte[msg.readableBytes()];
        msg.readBytes(bytes);
        final Input input = new Input(bytes);

        if(System.nanoTime()/Constants.NANO_TIME_DIVIDER - lastCommit >= 30)
        {
            lastCommit = System.nanoTime()/Constants.NANO_TIME_DIVIDER;
            writeToFile(aborts, committedTransactions, throughput, lastCommit);
            aborts = 0;
            committedTransactions = 0;
            throughput = 0;
        }

        final List returnValue = kryo.readObject(input, ArrayList.class);
        Log.getLogger().info("Received message!");
        committedTransactions++;
        for (Object obj : returnValue)
        {
            if (obj instanceof Operation)
            {
                ((Operation) obj).apply(access, 0);
                throughput++;
            }
            else if (obj instanceof NodeStorage || obj instanceof RelationshipStorage)
            {
                try(final Output output = new Output(0, 100024))
                {
                    kryo.writeObject(output, access.readObject(obj, 0));
                    ctx.writeAndFlush(output.getBuffer());
                    throughput++;
                }
                catch (OutDatedDataException e)
                {
                    Log.getLogger().info("Unable to retrieve data at clean server with instance: " + access.toString(), e);
                    aborts++;
                }
            }
        }
    }

    private void writeToFile(final int aborts, final int commits, final int throughput, final double time)
    {
        try(final FileWriter file = new FileWriter(System.getProperty("user.home") + "/results"+id+".txt", true);
            final BufferedWriter bw = new BufferedWriter(file);
            final PrintWriter out = new PrintWriter(bw))
        {
            out.println(time + ", ");
            out.print(aborts + ", ");
            out.print(commits + ", ");
            out.print(String.valueOf(throughput));
            out.println();
        }
        catch (IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
    }

    /**
     * Main method to start the clean server.
     *
     * @param args the arguments to start it with.
     */
    public static void main(String[] args)
    {
        if (args.length < 3)
        {
            return;
        }

        final int serverPort = Integer.parseInt(args[0]);
        final int id = Integer.parseInt(args[1]);
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

        if(args.length>=4)
        {
            boolean useLogging = Boolean.parseBoolean(args[3]);
            if(!useLogging)
            {
                Log.getLogger().setLevel(Level.OFF);
            }
        }

        final IDatabaseAccess access = ServerWrapper.instantiateDBAccess(instance, 0);
        if (access == null)
        {
            throw new UnsupportedOperationException("Wrong instance for this server");
        }
        access.start();

        final ThreadFactory acceptFactory = new DefaultThreadFactory("accept");
        final ThreadFactory connectFactory = new DefaultThreadFactory("connect");
        final NioEventLoopGroup acceptGroup = new NioEventLoopGroup(1, acceptFactory, NioUdtProvider.BYTE_PROVIDER);
        final NioEventLoopGroup connectGroup = new NioEventLoopGroup(1, connectFactory, NioUdtProvider.BYTE_PROVIDER);

        // Configure the server.
        try
        {
            final ServerBootstrap boot = new ServerBootstrap();
            boot.group(acceptGroup, connectGroup)
                    .channelFactory(NioUdtProvider.BYTE_ACCEPTOR)
                    .option(ChannelOption.SO_BACKLOG, 10)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<UdtChannel>()
                    {
                        @Override
                        public void initChannel(final UdtChannel ch)
                                throws Exception
                        {
                            ch.pipeline().addLast(
                                    new LoggingHandler(LogLevel.INFO),
                                    new CleanServer(access, id));
                        }
                    });
            // Start the server.
            final ChannelFuture future = boot.bind(serverPort).sync();
            // Wait until the server socket is closed.
            future.channel().closeFuture().sync();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            Log.getLogger().info("Netty server got interrupted", e);
        }
        finally
        {
            // Shut down all event loops to terminate all threads.
            acceptGroup.shutdownGracefully();
            connectGroup.shutdownGracefully();
        }
    }
}
