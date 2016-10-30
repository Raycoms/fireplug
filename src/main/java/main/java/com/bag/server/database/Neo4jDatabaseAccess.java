package main.java.com.bag.server.database;

import main.java.com.bag.server.database.Interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import java.io.File;
import java.util.*;

/**
 * Class created to handle access to the neo4j database.
 */
public class Neo4jDatabaseAccess implements IDatabaseAccess
{
    private static final String BASE_PATH = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/Neo4jDB";
    /**
     * The graphDB object.
     */
    private GraphDatabaseService graphDb;
    private int id;
    /**
     * The path to the neo4j graphDB
     */

    @Override
    public void start(int id)
    {
        File DB_PATH = new File(BASE_PATH + id);

        this.id = id;
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH)
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
    public void startReadOnly(int id)
    {
        File dbPath = new File(BASE_PATH + id);

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbPath )
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

    /**
     * Creates a transaction which will get a list of nodes.
     * @param identifier the nodes which should be retrieved.
     * @return the result nodes as a List of NodeStorages..
     */
    @NotNull
    public List<Object> readObject(@NotNull Object identifier)
    {
        NodeStorage nodeStorage = null;
        RelationshipStorage relationshipStorage =  null;

        if(identifier instanceof NodeStorage)
        {
            nodeStorage = (NodeStorage) identifier;
        }
        else if(identifier instanceof RelationshipStorage)
        {
            relationshipStorage = (RelationshipStorage) identifier;
        }
        else
        {
            Log.getLogger().warn("Can't read data on object: " + identifier.getClass().toString());
            return Collections.emptyList();
        }

        if(graphDb == null)
        {
            start(id);
        }

        //We only support 1 label each node/vertex because of compatibility with our graph dbs.
        ArrayList<Object> returnStorage =  new ArrayList<>();
        try(Transaction tx = graphDb.beginTx())
        {
            StringBuilder builder = new StringBuilder("MATCH ");

            if(nodeStorage == null)
            {
                builder.append(buildRelationshipString(relationshipStorage));
            }
            else
            {
                builder.append(buildNodeString(nodeStorage));
            }

            builder.append(" RETURN r");
            //todo validate result.
            Result result = graphDb.execute(builder.toString());

            //todo transfer result to NodeStorage or relationship storage or both.
            while (result.hasNext())
            {
                Object value = result.next();
                
                //todo check if result is of type relationship or node.
                NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                returnStorage.add(temp);
            }

            tx.success();
        }
        return returnStorage;
    }

    /**
     * Creates a Neo4j cypher String for a certain relationshipStorage
     * @param relationshipStorage the relationshipStorage to transform.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildRelationshipString(final RelationshipStorage relationshipStorage)
    {
        StringBuilder builder = new StringBuilder(buildNodeString(relationshipStorage.getStartNode()));

        builder.append("-[r");

        if(!relationshipStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s {", relationshipStorage.getId()));
        }

        if(relationshipStorage.getProperties() != null && !relationshipStorage.getProperties().isEmpty())
        {
            relationshipStorage.getProperties().entrySet().iterator();
            Iterator<Map.Entry<String, Object>> propertyIterator = relationshipStorage.getProperties().entrySet().iterator();

            while (propertyIterator.hasNext())
            {
                Map.Entry<String, Object> currentProperty = propertyIterator.next();
                builder.append(String.format("%s: '%s'",currentProperty.getKey(), currentProperty.getValue().toString()));

                if(propertyIterator.hasNext())
                {
                    builder.append(" , ");
                }
            }
            builder.append("}");
        }
        builder.append("]-");

        builder.append(buildNodeString(relationshipStorage.getEndNode()));

        return builder.toString();
    }

    /**
     * Creates a Neo4j cypher String for a certain nodeStorage.
     * @param nodeStorage the nodeStorage to transform.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildNodeString(NodeStorage nodeStorage)
    {
        StringBuilder builder = new StringBuilder("(n");

        if(!nodeStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s {", nodeStorage.getId()));
        }

        if(nodeStorage.getProperties() != null)
        {
            nodeStorage.getProperties().entrySet().iterator();
            Iterator<Map.Entry<String, Object>> propertyIterator = nodeStorage.getProperties().entrySet().iterator();

            while (propertyIterator.hasNext())
            {
                Map.Entry<String, Object> currentProperty = propertyIterator.next();
                builder.append(String.format("%s: '%s'",currentProperty.getKey(), currentProperty.getValue().toString()));

                if(propertyIterator.hasNext())
                {
                    builder.append(" , ");
                }
            }
        }
        builder.append("})");

        return builder.toString();
    }

    /**
     * Creates a transaction which will get all nodes.
     * @return all nodes as a List of NodeStorages.
     */
    @NotNull
    public List<NodeStorage> randomRead()
    {
        if(graphDb == null)
        {
            start(id);
        }
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
