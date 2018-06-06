package com.bag.server;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;

import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import com.bag.exceptions.OutDatedDataException;
import com.bag.instrumentations.ServerInstrumentation;
import com.bag.main.DatabaseLoader;
import com.bag.operations.CreateOperation;
import com.bag.operations.DeleteOperation;
import com.bag.operations.IOperation;
import com.bag.operations.UpdateOperation;
import com.bag.database.interfaces.IDatabaseAccess;
import com.bag.reconfiguration.sensors.LoadSensor;
import com.bag.server.nettyhandlers.BAGMessage;
import com.bag.server.nettyhandlers.BAGMessageDecoder;
import com.bag.server.nettyhandlers.BAGMessageEncoder;
import com.bag.util.Log;
import com.bag.util.storage.NodeStorage;
import com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.DeadlockDetectedException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Server used to communicate with graph databases directly without the use of BAG.
 */
public class CleanServer extends SimpleChannelInboundHandler<BAGMessage>
{
    /**
     * Name of the location of the global config.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global/config";

    /**
     * Create a threadsafe version of kryo.
     */
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
     * The database access for this class.
     */
    private final IDatabaseAccess access;

    /**
     * Used to measure and save performance info
     */
    private final ServerInstrumentation instrumentation;

    /**
     * A lock.
     */
    private final Object lock = new Object();

    /**
     * Create an instance of this server.
     *
     * @param access the instance of the db.
     */
    private CleanServer(final IDatabaseAccess access, final int id, final ServerInstrumentation instrumentation)
    {
        this.access = access;
        this.instrumentation = instrumentation;

        try (final FileWriter file = new FileWriter(System.getProperty("user.home") + "/results" + id + ".txt", true);
             final BufferedWriter bw = new BufferedWriter(file);
             final PrintWriter out = new PrintWriter(bw))
        {
            out.println();
            out.println("Starting new experiment (direct server): ");
            out.println();
            out.print("time;");
            out.print("aborts;");
            out.print("commits;");
            out.print("reads;");
            out.print("writes;");
            out.print("throughput");
            out.println();
        }
        catch (final IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
        Log.getLogger().setLevel(Level.WARN);
    }

    /**
     * Terminate the database access.
     */
    private void terminate()
    {
        access.terminate();
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
    {
        Log.getLogger().error("Exception caused in transfer", cause);
        ctx.close();
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final BAGMessage msg)
    {
        Log.getLogger().info("Received message!");
        synchronized (lock)
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            final Input input = new Input(msg.buffer);
            final List<Object> readObjects = new ArrayList<>();
            final int clientId = kryo.readObject(input, Integer.class);
            final List returnValue = kryo.readObject(input, ArrayList.class);

            final RSAKeyLoader rsaLoader = new RSAKeyLoader(0, GLOBAL_CONFIG_LOCATION, false);


            for (final Object obj : returnValue)
            {
                if (obj instanceof IOperation)
                {
                    Log.getLogger().info("Starting write!");
                    try
                    {
                        ((IOperation) obj).apply(access, OutDatedDataException.IGNORE_SNAPSHOT, rsaLoader, clientId);
                        instrumentation.updateCounts(1, 0, 0, 0);
                    }
                    catch (final DeadlockDetectedException e)
                    {
                        instrumentation.updateCounts(0, 0, 0, 1);
                        Log.getLogger().info("Dead-lock: ", e);
                    }
                    catch (final TransactionTerminatedException e)
                    {
                        instrumentation.updateCounts(0, 0, 0, 1);
                        Log.getLogger().info("Transaction terminated: ", e);
                    }
                    catch (final Exception e)
                    {
                        instrumentation.updateCounts(0, 0, 0, 1);
                        Log.getLogger().error("Unable to write data at clean server with instance: " + access.toString());
                    }
                }
                else if (obj instanceof NodeStorage || obj instanceof RelationshipStorage)
                {
                    Log.getLogger().info("Starting read!");
                    try
                    {
                        final List<Object> read = access.readObject(obj, OutDatedDataException.IGNORE_SNAPSHOT, clientId);
                        readObjects.addAll(read);
                        instrumentation.updateCounts(0, 1, 0, 0);
                    }
                    catch (final Exception e)
                    {
                        instrumentation.updateCounts(0, 0, 0, 1);
                        Log.getLogger().info("Unable to retrieve data at clean server with instance: " + access.toString(), e);
                    }
                }
                else
                {
                    Log.getLogger().info("Got commit!");
                }
            }
            instrumentation.updateCounts(0, 0, 1, 0);

            readObjects.add(new DeleteOperation<>());

            try (final Output output = new Output(0, 10240000))
            {
                kryo.writeObject(output, readObjects);
                final BAGMessage message = new BAGMessage();
                message.buffer = output.getBuffer();
                message.size = message.buffer.length;
                pool.release(kryo);
                ctx.writeAndFlush(message);
            }
            catch (final Exception ex)
            {
                Log.getLogger().warn("Error responding to client!", ex);
            }
        }
        Log.getLogger().info("Finished server execution, preparing response!");
    }

    /**
     * Main method to start the clean server.
     *
     * @param args the arguments to start it with.
     */
    public static void main(final String[] args)
    {
        if (args.length < 3)
        {
            Log.getLogger().error("Usage: CleanServer serverPort id databaseType");
            return;
        }

        LogManager.getRootLogger().setLevel(Level.WARN);

        final int serverPort = Integer.parseInt(args[0]);
        final int id = Integer.parseInt(args[1]);
        final String tempInstance = args[2];
        final ServerInstrumentation instrumentation = new ServerInstrumentation(id);

        try
        {
            Log.getLogger().warn("Setting up clean server with port: " + serverPort + " at id: " + id + " at: " + InetAddress.getLocalHost().toString());
        }
        catch (final UnknownHostException e)
        {
            e.printStackTrace();
        }
        final IDatabaseAccess access = DatabaseLoader.instantiateDBAccess(tempInstance.toLowerCase(), id, false, null, args[3]);
        if (args.length >= 4)
        {
            final boolean useLogging = Boolean.parseBoolean(args[3]);
            if (!useLogging)
            {
                Log.getLogger().setLevel(Level.OFF);
            }
        }

        access.start();

        final NioEventLoopGroup acceptGroup = new NioEventLoopGroup();
        final NioEventLoopGroup connectGroup = new NioEventLoopGroup();

        // Configure the server.
        try
        {
            final ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(acceptGroup, connectGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>()
                    {
                        protected void initChannel(final SocketChannel sc) throws Exception
                        {
                            sc.pipeline().addLast(
                                    new LoggingHandler(),
                                    new BAGMessageEncoder(),
                                    new BAGMessageDecoder(),
                                    new CleanServer(access, id, instrumentation));
                        }
                    });

            // Start the server.
            Log.getLogger().error("Direct server " + id + " started at port: " + serverPort);
            final ChannelFuture future = serverBootstrap.bind(serverPort).sync();
            // Wait until the server socket is closed.

            future.channel().closeFuture().sync();
            Log.getLogger().error("Closed already, wth!");
        }
        catch (final Exception e)
        {
            Log.getLogger().warn("Netty server ran into a issue", e);
        }
        finally
        {
            // Shut down all event loops to terminate all threads.
            acceptGroup.shutdownGracefully();
            connectGroup.shutdownGracefully();
        }
    }
}
