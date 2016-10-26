package main.java.com.bag.server.database;

import main.java.com.bag.server.database.Interfaces.IDatabaseAccess;
import main.java.com.bag.util.NodeStorage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Class created to handle access to the neo4j database.
 */
public class Neo4jDatabaseAccess implements IDatabaseAccess
{
    /**
     * The graphDB object.
     */
    private GraphDatabaseService graphDb;

    /**
     * The path to the neo4j graphDB
     */
    private static final File DB_PATH = new File("/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/Neo4jDB");

    @Override
    public void start()
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH )
                .setConfig(GraphDatabaseSettings.allow_store_upgrade, "true")
                .newGraphDatabase();

        registerShutdownHook( graphDb );
    }

    @Override
    public void terminate()
    {
        graphDb.shutdown();
    }

    /**
     * Starts the graph database in readOnly mode.
     */
    public void startReadOnly()
    {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH )
                .setConfig( GraphDatabaseSettings.read_only, "true" )
                .newGraphDatabase();
    }

    //todo create, read, update, delete.
    public void startTransaction(int snapshotId)
    {
        long calculateHash = 439508938;
        //todo node needs hash and snapshotId
        try(Transaction tx = graphDb.beginTx())
        {
            Node myNode = graphDb.createNode();
            myNode.setProperty( "name", "my node" );

            myNode.setProperty( "snapshot-id", snapshotId );
            myNode.setProperty( "node-hash", calculateHash );

            tx.success();
        }
    }

    public List<NodeStorage> randomRead()
    {
        ArrayList<NodeStorage> storage =  new ArrayList<>();
        try(Transaction tx = graphDb.beginTx())
        {
            ResourceIterable<Node> list = graphDb.getAllNodes();

            for(Node n: list)
            {
                NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                storage.add(temp);
            }

            tx.success();
        }
        return storage;
    }

    /**
     * Registers a shutdown hook for the Neo4j instance so that it
     * shuts down nicely when the VM exits (even if you "Ctrl-C" the
     * running application).
     * @param graphDb the graphDB to register the shutDownHook to.
     */
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
