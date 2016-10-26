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

        /*try(Transaction tx = graphDb.beginTx())
        {
            graphDb.execute("CREATE\n"
                    + "(leyla: Officer {name:\"Leyla Aliyeva\"})-[:IOO_BSD]->(ufu:Company {name:\"UF Universe Foundation\"}),\n"
                    + "(mehriban: Officer {name:\"Mehriban Aliyeva\"})-[:IOO_PROTECTOR]->(ufu),\n"
                    + "(arzu: Officer {name:\"Arzu Aliyeva\"})-[:IOO_BSD]->(ufu),\n"
                    + "(mossack_uk: Client {name:\"Mossack Fonseca & Co (UK)\"})-[:REGISTERED]->(ufu),\n"
                    + "(mossack_uk)-[:REGISTERED]->(fm_mgmt: Company {name:\"FM Management Holding Group S.A.\"}),\n"
                    + "\n"
                    + "(leyla)-[:IOO_BSD]->(kingsview:Company {name:\"Kingsview Developents Limited\"}),\n"
                    + "(leyla2: Officer {name:\"Leyla Ilham Qizi Aliyeva\"}),\n"
                    + "(leyla3: Officer {name:\"LEYLA ILHAM QIZI ALIYEVA\"})-[:HAS_SIMILIAR_NAME]->(leyla),\n"
                    + "(leyla2)-[:HAS_SIMILIAR_NAME]->(leyla3),\n"
                    + "(leyla2)-[:IOO_BENEFICIARY]->(exaltation:Company {name:\"Exaltation Limited\"}),\n"
                    + "(leyla3)-[:IOO_SHAREHOLDER]->(exaltation),\n"
                    + "(arzu2:Officer {name:\"Arzu Ilham Qizi Aliyeva\"})-[:IOO_BENEFICIARY]->(exaltation),\n"
                    + "(arzu2)-[:HAS_SIMILIAR_NAME]->(arzu),\n"
                    + "(arzu2)-[:HAS_SIMILIAR_NAME]->(arzu3:Officer {name:\"ARZU ILHAM QIZI ALIYEVA\"}),\n"
                    + "(arzu3)-[:IOO_SHAREHOLDER]->(exaltation),\n"
                    + "(arzu)-[:IOO_BSD]->(exaltation),\n"
                    + "(leyla)-[:IOO_BSD]->(exaltation),\n"
                    + "(arzu)-[:IOO_BSD]->(kingsview),\n"
                    + "\n"
                    + "(redgold:Company {name:\"Redgold Estates Ltd\"}),\n"
                    + "(:Officer {name:\"WILLY & MEYRS S.A.\"})-[:IOO_SHAREHOLDER]->(redgold),\n"
                    + "(:Officer {name:\"LONDEX RESOURCES S.A.\"})-[:IOO_SHAREHOLDER]->(redgold),\n"
                    + "(:Officer {name:\"FAGATE MINING CORPORATION\"})-[:IOO_SHAREHOLDER]->(redgold),\n"
                    + "(:Officer {name:\"GLOBEX INTERNATIONAL LLP\"})-[:IOO_SHAREHOLDER]->(redgold),\n"
                    + "(:Client {name:\"Associated Trustees\"})-[:REGISTERED]->(redgold)");

            tx.success();
        }*/

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
