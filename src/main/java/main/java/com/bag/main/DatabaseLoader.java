package main.java.com.bag.main;

import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.OrientDBDatabaseAccess;
import main.java.com.bag.server.database.SparkseeDatabaseAccess;
import main.java.com.bag.server.database.TitanDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;

/**
 * Loads databases with file stream.
 */
public class DatabaseLoader
{

    /**
     * The database access object.
     */
    private final IDatabaseAccess dbAccess;

    /**
     * Location of the graph.
     */
    private final String          graphLocation;

    /**
     * Should the id become label or default label?
     */
    private final boolean         idAsLabel;

    /**
     * Static label used for BAg.
     */
    private static final String LABEL = "BAGNode";

    public DatabaseLoader(IDatabaseAccess dbAcces, String graphLocation, boolean idAsLabel)
    {
        this.dbAccess = dbAcces;
        this.graphLocation = graphLocation;
        this.idAsLabel = idAsLabel;
    }

    public void loadGraph() throws Throwable
    {
        HashSet<Integer> cache = new HashSet<>();

        try (FileReader fr = new FileReader(graphLocation); BufferedReader br = new BufferedReader(fr);)
        {
            String line;
            int nodeOperations = 0;
            int relOperations = 0;
            int lastNodeOperations = 0;
            int lastRelOperations = 0;
            int count = 0;
            long nanos = System.nanoTime();
            long totalNanos = nanos;
            while ((line = br.readLine()) != null)
            {
                String[] fields = line.split(" ");
                if (fields.length < 3)
                {
                    continue;
                }

                int origin = Integer.parseInt(fields[0]);
                int destination = Integer.parseInt(fields[2]);

                NodeStorage nodeOrigin = createNode(fields[0]);
                NodeStorage nodeDest = createNode(fields[2]);

                if (!cache.contains(origin))
                {
                    dbAccess.applyCreate(nodeOrigin, 1);
                    nodeOperations += 1;
                    count += 1;
                    cache.add(origin);
                }

                if (!cache.contains(destination))
                {
                    dbAccess.applyCreate(nodeDest, 1);
                    nodeOperations += 1;
                    count += 1;
                    cache.add(destination);
                }

                RelationshipStorage rel = new RelationshipStorage(fields[1], nodeOrigin, nodeDest);
                dbAccess.applyCreate(rel, 1);

                /*nodeOrigin = createNode(fields[0]);
                nodeDest = createNode(fields[2]);
                rel = new RelationshipStorage(fields[1], nodeOrigin, nodeDest);
                List<Object> lst = dbAccess.readObject(rel, 1);
                for (Object l : lst)
                    System.out.printf("%s\n", l.toString());

                lst = dbAccess.readObject(nodeDest, 1);
                for (Object l : lst)
                    System.out.printf("%s\n", l.toString());*/

                relOperations += 1;
                count += 1;

                if (count >= 1000)
                {
                    count = 0;
                    double dif = (System.nanoTime() - nanos) / 1000000000.0;
                    System.out.printf("Time: %.3f s%nNodes: %d (%d new)%nRelations: %d (%d new)%n%n", dif, nodeOperations,
                            nodeOperations - lastNodeOperations, relOperations, relOperations - lastRelOperations);
                    lastNodeOperations = nodeOperations;
                    lastRelOperations = relOperations;
                    nanos = System.nanoTime();
                }
            }

            double dif = (System.nanoTime() - totalNanos) / 1000000000.0;
            System.out.printf("---------FINISHED--------%nTotal Time: %.3f s%nNodes: %d (%d new)%nRelations: %d (%d new)%n%n", dif, nodeOperations,
                    nodeOperations - lastNodeOperations, relOperations, relOperations - lastRelOperations);
        }
    }

    private NodeStorage createNode(String id)
    {
        if (idAsLabel)
        {
            return new NodeStorage(id);
        }
        else
        {
            NodeStorage result = new NodeStorage(LABEL);
            result.addProperty("Id", id);
            return result;
        }
    }

    /**
     * Instantiate the database access.
     * @param instance the instance to use.
     * @param globalServerId the global server id (used to find the folder)
     * @return the access object.
     */
    @NotNull
    private static IDatabaseAccess instantiateDBAccess(@NotNull final String instance, final int globalServerId)
    {
        switch (instance)
        {
            case Constants.NEO4J:
                return new Neo4jDatabaseAccess(globalServerId);
            case Constants.TITAN:
                return new TitanDatabaseAccess(globalServerId);
            case Constants.SPARKSEE:
                return new SparkseeDatabaseAccess(globalServerId);
            case Constants.ORIENTDB:
                return new OrientDBDatabaseAccess(globalServerId);
            default:
                Log.getLogger().warn("Invalid databaseAccess - default to Neo4j.");
                return new Neo4jDatabaseAccess(globalServerId);
        }
    }

    public static void main(String[] args)
    {
        String databaseId = "neo4";
        boolean idAsLabel = true;
        if (args.length > 0)
        {
            databaseId = args[0];
        }
        if (args.length > 1)
        {
            idAsLabel = Boolean.parseBoolean(args[1]);
        }


        Log.getLogger().setLevel(Level.WARN);

        IDatabaseAccess access = instantiateDBAccess(databaseId, 0);
        System.out.printf("Starting %s database%n", databaseId);
        access.start();
        System.out.printf("Loading...");

        DatabaseLoader loader = new DatabaseLoader(access, "/home/daniel/ray/thesis/src/testGraphs/social-a-graph.txt", idAsLabel);
        try
        {
            loader.loadGraph();
        }
        catch (Throwable throwable)
        {
            throwable.printStackTrace();
        }
        System.out.printf("Closing %s database%n", databaseId);
        access.terminate();
    }
}
