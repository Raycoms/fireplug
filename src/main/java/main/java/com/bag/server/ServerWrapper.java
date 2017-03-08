package main.java.com.bag.server;

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
     * The instance of the server which responds to the global cluster.
     * If null -> only slave of local cluster.
     */
    @Nullable private GlobalClusterSlave globalCluster;

    /**
     * The instance of the server which responds to the local cluster.
     */
    private LocalClusterSlave  localCluster;

    /**
     * Creates a serverWrapper which contains the instances of the global and local clusters.
     * Can be called by main or when a replica recovers by itself.
     * @param serverId the id of the server in the global cluster and local cluster.
     * @param instance  the instance of the server (Ex: Neo4j, OrientDB etc).
     */
    public ServerWrapper(final int serverId, @NotNull final String instance, boolean isPrimary)
    {
        if(isPrimary)
        {
            globalCluster = new GlobalClusterSlave(serverId, instance, this);
        }
        localCluster = new LocalClusterSlave(serverId, instance, this);
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
        //todo, we need more arguments
        //todo we need to know the local cluster it is part of and if its the primary which should be part of the global cluster.
        //todo all ids are globally unique.
        int serverId = 0;
        String instance = Constants.NEO4J;

        if(args.length == 1)
        {
            try
            {
                serverId = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException ne)
            {
                Log.getLogger().warn("Invalid program arguments, terminating server");
                return;
            }
        }
        else if(args.length == 2)
        {
            try
            {
                serverId = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException ne)
            {
                Log.getLogger().warn("Invalid program arguments, terminating server");
                return;
            }

            String tempInstance = args[1];

            if(tempInstance.toLowerCase().contains("titan"))
            {
                instance = Constants.TITAN;
            }
            else if(tempInstance.toLowerCase().contains("orientdb"))
            {
                instance = Constants.ORIENTDB;
            }
            else if(tempInstance.toLowerCase().contains("sparksee"))
            {
                instance = Constants.SPARKSEE;
            }
            else
            {
                instance = Constants.NEO4J;
            }
        }

        @NotNull final ServerWrapper wrapper = new ServerWrapper(serverId, instance, true);

        Scanner reader = new Scanner(System.in);  // Reading from System.in
        Log.getLogger().info("Write anything to the console to kill this process");
        String command = reader.next();

        if(command != null)
        {
            wrapper.terminate();
        }
    }
}
