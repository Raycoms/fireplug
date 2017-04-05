package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.netty.bootstrap.ServerBootstrap;
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
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.OrientDBDatabaseAccess;
import main.java.com.bag.server.database.SparkseeDatabaseAccess;
import main.java.com.bag.server.database.TitanDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.server.nettyhandlers.BAGMessage;
import main.java.com.bag.server.nettyhandlers.BAGMessageDecoder;
import main.java.com.bag.server.nettyhandlers.BAGMessageEncoder;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

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
public class CleanServer extends SimpleChannelInboundHandler<BAGMessage>
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
     * Id of the server.
     */
    private final int id;


    /**
     * Used to measure and save performance info
     */
    private ServerInstrumentation instrumentation;

    /**
     * Create an instance of this server.
     *
     * @param access the instance of the db.
     */
    private CleanServer(final IDatabaseAccess access, final int id, ServerInstrumentation instrumentation)
    {
        this.access = access;
        this.id = id;
        this.instrumentation = instrumentation;

        try(final FileWriter file = new FileWriter(System.getProperty("user.home") + "/resultsd"+id+".txt", true);
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
        catch (IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
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
    public void channelRead0(final ChannelHandlerContext ctx, final BAGMessage msg) {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        final Input input = new Input(msg.buffer);
        int writesPerformed = 0;
        List<Object> readObjects = new ArrayList<>();
        final List returnValue = kryo.readObject(input, ArrayList.class);
        Log.getLogger().info("Received message!");
        for (Object obj : returnValue) {
            if (obj instanceof Operation) {
                ((Operation) obj).apply(access, OutDatedDataException.IGNORE_SNAPSHOT);
                instrumentation.updateCounts(1, 0, 0, 0);
                writesPerformed += 1;
            } else if (obj instanceof NodeStorage || obj instanceof RelationshipStorage) {
                try {
                    List<Object> read = access.readObject(obj, OutDatedDataException.IGNORE_SNAPSHOT);
                    readObjects.addAll(read);
                    instrumentation.updateCounts(0, read.size(), 0, 0);
                } catch (OutDatedDataException e) {
                    Log.getLogger().info("Unable to retrieve data at clean server with instance: " + access.toString(), e);
                    instrumentation.updateCounts(0, 0, 0, 1);
                }
            }
        }

        readObjects.add(new DeleteOperation<>());

        if (writesPerformed > 0)
            instrumentation.updateCounts(0, 0, 1, 0);

        try (final Output output = new Output(0, 1024 * 100)) {
            kryo.writeObject(output, readObjects);
            kryo.writeObject(output, new DeleteOperation<NodeStorage>());
            BAGMessage message = new BAGMessage();
            message.buffer = output.getBuffer();
            message.size = message.buffer.length;
            pool.release(kryo);
            ctx.writeAndFlush(message);
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
            System.out.println("Usage: CleanServer serverPort id databaseType");
            return;
        }

        LogManager.getRootLogger().setLevel(Level.WARN);

        final int serverPort = Integer.parseInt(args[0]);
        final int id = Integer.parseInt(args[1]);
        final String tempInstance = args[2];

        IDatabaseAccess access;

        if (tempInstance.toLowerCase().contains("titan"))
        {
            access = new TitanDatabaseAccess(id);
        }
        else if (tempInstance.toLowerCase().contains("orientdb"))
        {
            access = new OrientDBDatabaseAccess(id);
        }
        else if (tempInstance.toLowerCase().contains("sparksee"))
        {
            access = new SparkseeDatabaseAccess(id);
        }
        else
        {
            access = new Neo4jDatabaseAccess(id, args[3]);
        }

        if(args.length>=4)
        {
            boolean useLogging = Boolean.parseBoolean(args[3]);
            if(!useLogging)
            {
                Log.getLogger().setLevel(Level.OFF);
            }
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
            ServerInstrumentation instrumentation = new ServerInstrumentation(id);
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
                                    new LoggingHandler(),
                                    new BAGMessageEncoder(),
                                    new BAGMessageDecoder(),
                                    new CleanServer(access, id, instrumentation));
                        }
                    });
            // Start the server.
            final ChannelFuture future = boot.bind(serverPort).sync();
            // Wait until the server socket is closed.
            System.out.println("Direct server " + id + " started.");
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
