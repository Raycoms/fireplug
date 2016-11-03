package main.java.com.bag.server.database;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

import java.io.File;
import java.util.*;

/**
 * Class created to handle access to the neo4j database.
 */
public class Neo4jDatabaseAccess implements IDatabaseAccess
{
    private static final String BASE_PATH    = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/Neo4jDB";
    /**
     * The graphDB object.
     */
    private GraphDatabaseService graphDb;

    /**
     * Id of the database. (If multiple running on the same machine.
     */
    private int id;

    /**
     * String used to match key value pairs.
     */
    private static final String KEY_VALUE_PAIR = "%s: '%s'";

    /**
     * Match string for cypher queries.
     */
    private static final String MATCH = "MATCH ";

    @Override
    public void start(int id)
    {
        File DB_PATH = new File(BASE_PATH + id);

        this.id = id;
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH).newGraphDatabase();
        registerShutdownHook( graphDb );

        try(Transaction tx = graphDb.beginTx())
        {
            /*graphDb.execute("CREATE\n"
                    + "(leyla: Officer {name:\"Leyla Aliyeva\"})-[:IOO_BSD]->(ufu:Company {name:\"UF Universe Foundation\", snapShotId: '0'}),\n"
                    + "(mehriban: Officer {name:\"Mehriban Aliyeva\"})-[:IOO_PROTECTOR]->(ufu),\n"
                    + "(arzu: Officer {name:\"Arzu Aliyeva\"})-[:IOO_BSD]->(ufu),\n"
                    + "(mossack_uk: Client {name:\"Mossack Fonseca & Co (UK)\"})-[:REGISTERED]->(ufu),\n"
                    + "(mossack_uk)-[:REGISTERED]->(fm_mgmt: Company {name:\"FM Management Holding Group S.A.\", snapShotId: '0'}),\n"
                    + "\n"
                    + "(leyla)-[:IOO_BSD]->(kingsview:Company {name:\"Kingsview Developents Limited\"}),\n"
                    + "(leyla2: Officer {name:\"Leyla Ilham Qizi Aliyeva\"}),\n"
                    + "(leyla3: Officer {name:\"LEYLA ILHAM QIZI ALIYEVA\"})-[:HAS_SIMILIAR_NAME]->(leyla),\n"
                    + "(leyla2)-[:HAS_SIMILIAR_NAME]->(leyla3),\n"
                    + "(leyla2)-[:IOO_BENEFICIARY]->(exaltation:Company {name:\"Exaltation Limited\", snapShotId: '0'}),\n"
                    + "(leyla3)-[:IOO_SHAREHOLDER]->(exaltation),\n"
                    + "(arzu2:Officer {name:\"Arzu Ilham Qizi Aliyeva\"})-[:IOO_BENEFICIARY]->(exaltation),\n"
                    + "(arzu2)-[:HAS_SIMILIAR_NAME]->(arzu),\n"
                    + "(arzu2)-[:HAS_SIMILIAR_NAME]->(arzu3:Officer {name:\"ARZU ILHAM QIZI ALIYEVA\", snapShotId: '0'}),\n"
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
                    + "(:Client {name:\"Associated Trustees\"})-[:REGISTERED]->(redgold)");*/

            tx.success();
        }

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
    public List<Object> readObject(@NotNull Object identifier, long snapShotId)
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
                Log.getLogger().info(Long.toString(snapShotId));
                builder.append(buildRelationshipString(relationshipStorage));
                builder.append(String.format(" WHERE r.%s <= %s OR n.%s IS NULL", Constants.TAG_SNAPSHOT_ID, Long.toString(snapShotId), Constants.TAG_SNAPSHOT_ID));
                builder.append(" RETURN r");
            }
            else
            {
                Log.getLogger().info(Long.toString(snapShotId));
                builder.append(buildNodeString(nodeStorage, ""));
                builder.append(String.format(" WHERE n.%s <= %s OR n.%s IS NULL",Constants.TAG_SNAPSHOT_ID, Long.toString(snapShotId), Constants.TAG_SNAPSHOT_ID));
                builder.append(" RETURN n");
            }

            Result result = graphDb.execute(builder.toString());
            while (result.hasNext())
            {
                Map<String, Object> value = result.next();

                for(Map.Entry<String, Object> entry: value.entrySet())
                {
                    if(entry.getValue() instanceof NodeProxy)
                    {
                        NodeProxy n = (NodeProxy) entry.getValue();
                        NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                        returnStorage.add(temp);
                    }
                    else if(entry.getValue() instanceof RelationshipProxy)
                    {
                        RelationshipProxy r = (RelationshipProxy) entry.getValue();
                        NodeStorage start = new NodeStorage(r.getStartNode().getLabels().iterator().next().name(), r.getStartNode().getAllProperties());
                        NodeStorage end = new NodeStorage(r.getEndNode().getLabels().iterator().next().name(), r.getEndNode().getAllProperties());


                        RelationshipStorage temp = new RelationshipStorage(r.getType().name(), r.getAllProperties(), start, end);
                        returnStorage.add(temp);
                    }
                }
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
        StringBuilder builder = new StringBuilder(buildNodeString(relationshipStorage.getStartNode(), "1"));

        builder.append("-[r");

        if (!relationshipStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s", relationshipStorage.getId()));
        }

        builder.append(" {");
        Iterator<Map.Entry<String, Object>> iterator = relationshipStorage.getProperties().entrySet().iterator();

        while (iterator.hasNext())
        {
            Map.Entry<String, Object> currentProperty = iterator.next();
            builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getValue().toString()));

            if(iterator.hasNext())
            {
                builder.append(" , ");
            }
        }

        builder.append("}]-");

        builder.append(buildNodeString(relationshipStorage.getEndNode(), "2"));

        return builder.toString();
    }

    /**
     * Creates a Neo4j cypher String for a certain nodeStorage.
     * @param nodeStorage the nodeStorage to transform.
     * @param n optional identifier in the query.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildNodeString(NodeStorage nodeStorage, String n)
    {
        StringBuilder builder = new StringBuilder("(n").append(n);

        if (!nodeStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s", nodeStorage.getId()));
        }
        builder.append(" {");

        Iterator<Map.Entry<String, Object>> iterator = nodeStorage.getProperties().entrySet().iterator();

        while(iterator.hasNext())
        {
            Map.Entry<String, Object> currentProperty = iterator.next();
            builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getValue().toString()));

            if(iterator.hasNext())
            {
                builder.append(" , ");
            }
        }
        builder.append("})");
        return builder.toString();
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

    @Override
    public boolean equalHash(final List readSet)
    {
        if(readSet.isEmpty())
        {
            return true;
        }

        if(readSet.get(0) instanceof NodeStorage)
        {
            equalHashNode(readSet);
        }
        else if(readSet.get(0) instanceof RelationshipStorage)
        {

        }

        return true;
    }

    private boolean equalHashNode(final List readSet)
    {
        for(Object storage: readSet)
        {
            if(storage instanceof NodeStorage)
            {
                NodeStorage nodeStorage = (NodeStorage) storage;

                try (Transaction tx = graphDb.beginTx())
                {
                    StringBuilder builder = new StringBuilder("MATCH ");

                    builder.append(buildNodeString(nodeStorage, ""));
                    builder.append(" RETURN n");


                    Result result = graphDb.execute(builder.toString());

                    //todo can we assume we only get one node ? and this node has to fit?
                    if(result.hasNext())
                    {
                        Map<String, Object> value = result.next();

                        for (Map.Entry<String, Object> entry : value.entrySet())
                        {
                            if (entry.getValue() instanceof NodeProxy)
                            {
                                NodeProxy n = (NodeProxy) entry.getValue();
                                //todo check if hash is the same as inside the nodeStorage.
                                NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                            }
                        }
                    }

                    tx.success();
                }
            }
        }
        return true;
    }

    private boolean equalHashRelationship(final List<NodeStorage> readSet)
    {

        return true;
    }
}
