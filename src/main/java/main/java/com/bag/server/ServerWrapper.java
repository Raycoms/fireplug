package main.java.com.bag.server;

import com.esotericsoftware.kryo.pool.KryoFactory;
import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.database.interfaces.IDatabaseAccess;
import main.java.com.bag.main.DatabaseLoader;
import main.java.com.bag.util.Log;
import org.apache.log4j.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Server wrapper class which will contain the instance of the local cluster and global cluster.
 */
public class ServerWrapper
{
    /**
     * String to print in the case of invalid arguments.
     */
    private static final String INVALID_ARGUMENTS = "Invalid program arguments, terminating server, expecting: <serverId> <DBInstance> <localSlaveId> <primaryID> <actsInGlobalCluster> [logging] [multiVersion] [globallyVerified]";

    /**
     * If the server operates under multiVersion mode or not.
     */
    private final boolean multiVersion;

    /**
     * If the server is globally verified or locally verified.
     */
    private final boolean globallyVerified;

    /**
     * The instance of the server which responds to the global cluster.
     * If null -> only slave of local cluster.
     */
    @Nullable private GlobalClusterSlave globalCluster;

    /**
     * The instance of the server which responds to the local cluster.
     */
    private LocalClusterSlave  localCluster;

    /**
     * Last executed transactionId.
     */
    private int lastTransactionId;

    /**
     * The id of the server in the global cluster. Also the id of the local cluster (initially).
     */
    private final int globalServerId;

    /**
     * The id of the server in the local cluster.
     */
    private final int localClusterSlaveId;

    /**
     * The timer class.
     */
    private final Timer timer = new Timer();

    /**
     * The database instance.
     */
    private final IDatabaseAccess databaseAccess;

    /**
     * Creates a serverWrapper which contains the instances of the global and local clusters.
     * Can be called by main or when a replica recovers by itself.
     * @param globalServerId the id of the server in the global cluster and local cluster.
     * @param instance  the instance of the server (Ex: Neo4j, OrientDB etc).
     * @param isPrimary checks if it is a primary.
     * @param localClusterSlaveId the id of it in the local cluster.
     * @param initialLeaderId the id of its leader in the global cluster.
     * @param multiVersion if multi-version mode.
     */
    public ServerWrapper(final int globalServerId,
                         @NotNull final String instance,
                         final boolean isPrimary,
                         final int localClusterSlaveId,
                         final int initialLeaderId,
                         final boolean multiVersion,
                         final boolean globallyVerified)
    {
        final ServerInstrumentation instrumentation = new ServerInstrumentation(globalServerId);
        this.globalServerId = globalServerId;
        this.localClusterSlaveId = localClusterSlaveId;
        lastTransactionId = 0;

        if (this.globalServerId == 1)
        {
            timer.schedule(new TimerTask() {
                @Override
                public void run()
                {
                    terminate();
                }
            }, 90000);
        }

        if(isPrimary)
        {
            Log.getLogger().error("Turn on global cluster with id: " + globalServerId);
            globalCluster = new GlobalClusterSlave(globalServerId, this, instrumentation, 0);
            Log.getLogger().error("Finished turning on global cluster with id: " + globalServerId);
        }

        if(localClusterSlaveId != -1)
        {
            Log.getLogger().error("Start local cluster slave with id: "  + localClusterSlaveId);
            localCluster = new LocalClusterSlave(localClusterSlaveId, this, initialLeaderId, instrumentation);
            if(isPrimary)
            {
                localCluster.setPrimaryGlobalClusterId(globalServerId);
            }
            else
            {
                localCluster.setPrimaryGlobalClusterId(initialLeaderId);
            }
            Log.getLogger().error("Finished turning on local cluster slave with id: " + localClusterSlaveId);
        }

        final KryoFactory pool = globalCluster != null ? globalCluster.getFactory() : localCluster.getFactory();

        databaseAccess = DatabaseLoader.instantiateDBAccess(instance, globalServerId, multiVersion, pool);
        databaseAccess.start();

        if(isPrimary && localClusterSlaveId != -1)
        {
            localCluster.setPrimary(true);
        }

        this.multiVersion = multiVersion;
        this.globallyVerified = globallyVerified;
    }

    /**
     * Get an instance of the dataBaseAccess.
     * @return the instance.
     */
    public synchronized IDatabaseAccess getDataBaseAccess()
    {
        return this.databaseAccess;
    }

    /**
     * Get the global cluster in this wrapper.
     * @return the global cluster.
     */
    @Nullable
    public GlobalClusterSlave getGlobalCluster()
    {
        return this.globalCluster;
    }

    /**
     * Getter to check if multiVersion.
     * @return true if so.
     */
    public boolean isMultiVersion()
    {
        return multiVersion;
    }

    /**
     * Getter to check if globallyVerified.
     * @return true if so.
     */
    public boolean isGloballyVerified()
    {
        return globallyVerified;
    }

    /**
     * Set the last transactionID.
     * @param newId the new ID to set.
     */
    public void setLastTransactionId(final int newId)
    {
        this.lastTransactionId = newId;
    }

    /**
     * Get the last transactionID.
     * @return the ID.
     */
    public int getLastTransactionId()
    {
        return this.lastTransactionId;
    }

    /**
     * Get the id of the server in the global cluster.
     * @return the id, an int.
     */
    public int getGlobalServerId()
    {
        return globalServerId;
    }

    /**
     * Kill global and local clusters
     */
    private void terminate()
    {
        if(globalCluster != null)
        {
            globalCluster.close();
        }
        if(localCluster != null)
        {
            localCluster.close();
        }
    }

    /**
     * Main method used to start each GlobalClusterSlave.
     * @param args the id for each testServer, set it in the program arguments.
     */
    public static void main(final String [] args)
    {
        /*
         * The server arguments are:
         * - ServerId (unique in global cluster)
         * - Instance (db to use, neo4j etc)
         * - Id in localCluster (-id if not needed)
         * - idOfPrimary
         * - ActsInGlobalCluster (p.e if is primary),
         * - Use Logging (true of false)
         * - MultiVersion
         * - GloballyVerified
         * Example: 0 neo4j 0 0 true [true false true]
         */


        final int serverId;
        final String instance;
        final int localClusterSlaveId;
        final int idOfPrimary;

        final boolean actsInGlobalCluster;

        if (args.length <= 3)
        {
            Log.getLogger().error(INVALID_ARGUMENTS);
            return;
        }

        try
        {
            serverId = Integer.parseInt(args[0]);
        }
        catch (final NumberFormatException ne)
        {
            Log.getLogger().error(INVALID_ARGUMENTS);
            return;
        }

        instance = args[1];

        try
        {
            localClusterSlaveId = Integer.parseInt(args[2]);
        }
        catch (final NumberFormatException ne)
        {
            Log.getLogger().error(INVALID_ARGUMENTS);
            return;
        }

        try
        {
            idOfPrimary = Integer.parseInt(args[3]);
        }
        catch (final NumberFormatException ne)
        {
            Log.getLogger().error(INVALID_ARGUMENTS);
            return;
        }

        actsInGlobalCluster = args.length > 4 && Boolean.parseBoolean(args[4]);

        if (args.length >= 6)
        {
            final boolean useLogging = Boolean.parseBoolean(args[5]);
            if (!useLogging)
            {
                Log.getLogger().setLevel(Level.WARN);
            }
        }

        boolean multiVersion = false;
        if (args.length >= 7)
        {
            multiVersion = Boolean.parseBoolean(args[6]);
            Log.getLogger().error("Starting server with multiVersion: " + multiVersion);
        }

        boolean globallyVerified = false;
        if (args.length >= 8)
        {
            globallyVerified = Boolean.parseBoolean(args[7]);
            Log.getLogger().error("Starting server globally verified: " + globallyVerified);
        }

        @NotNull final ServerWrapper wrapper = new ServerWrapper(serverId, instance, actsInGlobalCluster, localClusterSlaveId, idOfPrimary, multiVersion, globallyVerified);

        final Scanner reader = new Scanner(System.in);  // Reading from System.in
        Log.getLogger().info("Write <kill> to the console to kill this process");

        while(reader.hasNext())
        {
            final String command = reader.next();
            if (command != null && command.equals("kill"))
            {
                Log.getLogger().info("Killing server!");
                wrapper.terminate();
            }
        }
    }

    /**
     * Turn on a new instance of the global cluster.
     * @param lastBatch the last executed batch.
     */
    public void initNewGlobalClusterInstance(final long lastBatch)
    {
        Log.getLogger().warn("----------------------------------------------------");
        Log.getLogger().warn("Starting Replica: " + globalServerId + " in global Cluster!");
        Log.getLogger().warn("----------------------------------------------------");
        globalCluster = new GlobalClusterSlave(globalServerId, this, localCluster.getInstrumentation(), lastBatch);
    }

    /**
     * Turn on a new instance of the local cluster.
     */
    public void initNewLocalClusterInstance()
    {
        localCluster = new LocalClusterSlave(localClusterSlaveId, this, globalServerId, new ServerInstrumentation(globalServerId));
    }

    /**
     * Get an instance of the local cluster.
     * @return the local cluster instance.
     */
    public LocalClusterSlave getLocalCluster()
    {
        return localCluster;
    }

    /**
     * Close the global cluster instance.
     */
    public void terminateGlobalCluster()
    {
        if(globalCluster != null)
        {
            globalCluster.close();
            globalCluster = null;
        }
    }

    /**
     * Get the id of the local cluster.
     * @return the id of it.
     */
    public int getLocalClusterSlaveId()
    {
        return localClusterSlaveId;
    }


}
