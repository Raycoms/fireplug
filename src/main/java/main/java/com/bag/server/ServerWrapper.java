package main.java.com.bag.server;

import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.OrientDBDatabaseAccess;
import main.java.com.bag.server.database.SparkseeDatabaseAccess;
import main.java.com.bag.server.database.TitanDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Scanner;

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
     * The String instance of the database.
     */
    private final String instance;

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
     */
    public ServerWrapper(final int globalServerId, @NotNull final String instance, final boolean isPrimary, final int localClusterSlaveId, final int initialLeaderId)
    {
        this.globalServerId = globalServerId;
        this.instance = instance;
        this.localClusterSlaveId = localClusterSlaveId;

        instantiateDBAccess(instance);

        databaseAccess.start();

        localCluster = new LocalClusterSlave(localClusterSlaveId, this, globalServerId);
        if(isPrimary)
        {
            globalCluster = new GlobalClusterSlave(globalServerId, this);
            localCluster.setPrimary(true);
        }

        localCluster.setPrimaryGlobalClusterId(initialLeaderId);
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
     * Instantiate the Database access classes depending on the String instance.
     *
     * @param instance the string describing which to use.
     */
    public void instantiateDBAccess(String instance)
    {
        switch (instance)
        {
            case Constants.NEO4J:
                databaseAccess = new Neo4jDatabaseAccess(globalServerId);
                break;
            case Constants.TITAN:
                databaseAccess = new TitanDatabaseAccess(globalServerId);
                break;
            case Constants.SPARKSEE:
                databaseAccess = new SparkseeDatabaseAccess(globalServerId);
                break;
            case Constants.ORIENTDB:
                databaseAccess = new OrientDBDatabaseAccess(globalServerId);
                break;
            default:
                Log.getLogger().warn("Invalid databaseAccess");
        }
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
        //todo all ids are globally unique.
        final int serverId;
        final String instance;
        final boolean actsInGlobalCluster;
        final int localClusterSlaveId;
        final int idOfPrimary;

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
        else
        {
            instance = Constants.NEO4J;
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

        if (args.length == 4)
        {
            actsInGlobalCluster = false;
        }
        else
        {
            actsInGlobalCluster = Boolean.valueOf(args[2]);
        }
        @NotNull final ServerWrapper wrapper = new ServerWrapper(serverId, instance, actsInGlobalCluster, localClusterSlaveId, idOfPrimary);

        final Scanner reader = new Scanner(System.in);  // Reading from System.in
        Log.getLogger().info("Write anything to the console to kill this process");
        final String command = reader.next();

        if (command != null)
        {
            wrapper.terminate();
        }
    }

    /**
     * Turn on a new instance of the global cluster.
     */
    public void initNewGlobalClusterInstance()
    {
        globalCluster = new GlobalClusterSlave(globalServerId, this);
    }

    /**
     * Turn on a new instance of the local cluster.
     */
    public void initNewLocalClusterInstance()
    {
        localCluster = new LocalClusterSlave(localClusterSlaveId, this, globalServerId);
    }

    /**
     * Get an instance of the local cluster.
     * @return the local cluster instance.
     */
    public LocalClusterSlave getLocalCLuster()
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
