package main.java.com.bag.server;

import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.database.interfaces.IDatabaseAccess;
import main.java.com.bag.main.DatabaseLoader;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import org.apache.log4j.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Server wrapper class which will contain the instance of the local cluster and global cluster.
 */
public class ServerWrapper
{
    /**
     * String to print in the case of invalid arguments.
     */
    private static final String INVALID_ARGUMENTS = "Invalid program arguments, terminating server";

    /**
     * If the server operates under multiVersion mode or not.
     */
    private final boolean multiVersion;

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
     * The id of the server in the global cluster. Also the id of the local cluster (initially).
     */
    private final int globalServerId;

    /**
     * The id of the server in the local cluster.
     */
    private final int localClusterSlaveId;


    /**
     * The database instance.
     */
    private IDatabaseAccess databaseAccess;

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
    public ServerWrapper(final int globalServerId, @NotNull final String instance, final boolean isPrimary, final int localClusterSlaveId, final int initialLeaderId, final boolean multiVersion)
    {
        final ServerInstrumentation instrumentation = new ServerInstrumentation(globalServerId);
        this.globalServerId = globalServerId;
        this.localClusterSlaveId = localClusterSlaveId;

        databaseAccess = DatabaseLoader.instantiateDBAccess(instance, globalServerId, multiVersion);
        databaseAccess.start();

        if(isPrimary)
        {
            Log.getLogger().warn("Turn on global cluster with id: " + globalServerId);
            globalCluster = new GlobalClusterSlave(globalServerId, this, instrumentation);
            Log.getLogger().warn("Finished turning on global cluster with id: " + globalServerId);
        }

        if(localClusterSlaveId != -1)
        {
            Log.getLogger().warn("Start local cluster slave with id: "  + localClusterSlaveId);
            localCluster = new LocalClusterSlave(localClusterSlaveId, this, initialLeaderId, instrumentation);
            if(isPrimary)
            {
                localCluster.setPrimaryGlobalClusterId(globalServerId);
            }
            else
            {
                localCluster.setPrimaryGlobalClusterId(initialLeaderId);
            }
            Log.getLogger().warn("Finished turning on local cluster slave with id: " + localClusterSlaveId);
        }

        if(isPrimary && localClusterSlaveId != -1)
        {
            localCluster.setPrimary(true);
        }

        this.multiVersion = multiVersion;
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
            globalCluster.terminate();
        }
        if(localCluster != null)
        {
            localCluster.terminate();
        }
    }

    /**
     * Main method used to start each GlobalClusterSlave.
     * @param args the id for each testServer, set it in the program arguments.
     */
    public static void main(String [] args)
    {

        /*
         * The server arguments are:
         * serverId (unique in global cluster), instance (db to use, neo4j etc), id in localCluster, actsInGlobalCluster (p.e if is primary),
         * id of primary of local cluster (id of local cluster)
         * , use Logging (true of false)
         *
         * 0 neo4j 0 0 true true
         */
        final int serverId;
        final String instance;
        final int localClusterSlaveId;
        final int idOfPrimary;

        final boolean actsInGlobalCluster;

        if (args.length <= 3)
        {
            Log.getLogger().warn(INVALID_ARGUMENTS);
            return;
        }

        try
        {
            serverId = Integer.parseInt(args[0]);
        }
        catch (NumberFormatException ne)
        {
            Log.getLogger().warn(INVALID_ARGUMENTS);
            return;
        }

        final String tempInstance = args[1];

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
        else if(tempInstance.toLowerCase().contains("neo4j"))
        {
            instance = Constants.NEO4J;
        }
        else
        {
            instance = "none";
        }

        try
        {
            localClusterSlaveId = Integer.parseInt(args[2]);
        }
        catch (NumberFormatException ne)
        {
            Log.getLogger().warn(INVALID_ARGUMENTS);
            return;
        }

        try
        {
            idOfPrimary = Integer.parseInt(args[3]);
        }
        catch (NumberFormatException ne)
        {
            Log.getLogger().warn(INVALID_ARGUMENTS);
            return;
        }

        if (args.length <= 4)
        {
            actsInGlobalCluster = false;
        }
        else
        {
            actsInGlobalCluster = Boolean.parseBoolean(args[4]);
        }

        if(args.length>=6)
        {
            boolean useLogging = Boolean.parseBoolean(args[5]);
            if(!useLogging)
            {
                Log.getLogger().setLevel(Level.WARN);
            }
        }

        boolean multiVersion = false;
        if(args.length>=7)
        {
            multiVersion = Boolean.parseBoolean(args[6]);
            Log.getLogger().warn("Starting server with multiVersion: " + multiVersion);
        }

        @NotNull final ServerWrapper wrapper = new ServerWrapper(serverId, instance, actsInGlobalCluster, localClusterSlaveId, idOfPrimary, multiVersion);

        /*final Scanner reader = new Scanner(System.in);  // Reading from System.in
        Log.getLogger().info("Write anything to the console to kill this process");
        final String command = reader.next();

        if (command != null)
        {
            wrapper.terminate();
        }*/
    }

    /**
     * Turn on a new instance of the global cluster.
     */
    public void initNewGlobalClusterInstance()
    {
        globalCluster = new GlobalClusterSlave(globalServerId, this, new ServerInstrumentation(globalServerId));
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

    /**
     * Set the database access for this server.
     * Likely after an unexpected shutdown.
     * @param dataBaseAccess the access to set.
     */
    public void setDataBaseAccess(final IDatabaseAccess dataBaseAccess)
    {
        this.databaseAccess = dataBaseAccess;
    }
}
