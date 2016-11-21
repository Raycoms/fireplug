package main.java.com.bag.server.database;

import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

import java.io.File;
import java.security.NoSuchAlgorithmException;
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
    private final int id;

    /**
     * String used to match key value pairs.
     */
    private static final String KEY_VALUE_PAIR = "%s: '%s'";

    private static final String MATCH = "MATCH ";

    /**
     * Public constructor.
     * @param id, id of the server.
     */
    public Neo4jDatabaseAccess(int id)
    {
        this.id = id;
    }

    @Override
    public void start()
    {
        File dbPath = new File(BASE_PATH + id);
        Log.getLogger().info("Starting neo4j database service on " + id);

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).newGraphDatabase();
        registerShutdownHook( graphDb );
    }

    @Override
    public void terminate()
    {
        Log.getLogger().info("Shutting down Neo4j manually");
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

    /**
     * Creates a transaction which will get a list of nodes.
     * @param identifier the nodes which should be retrieved.
     * @return the result nodes as a List of NodeStorages..
     */
    @NotNull
    public List<Object> readObject(@NotNull Object identifier, long snapshotId) throws OutDatedDataException
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
            start();
        }

        //We only support 1 label each node/vertex because of compatibility with our graph dbs.
        ArrayList<Object> returnStorage =  new ArrayList<>();
        try(Transaction tx = graphDb.beginTx())
        {
            StringBuilder builder = new StringBuilder(MATCH);

            if(nodeStorage == null)
            {
                Log.getLogger().info(Long.toString(snapshotId));
                builder.append(buildRelationshipString(relationshipStorage));
                builder.append(" RETURN r");
            }
            else
            {
                Log.getLogger().info(Long.toString(snapshotId));
                builder.append(buildNodeString(nodeStorage, ""));
                builder.append(" RETURN n");
            }

            Log.getLogger().info("To database: " + builder.toString());

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
                        if(temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            Object sId =  temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            OutDatedDataException.checkSnapshotId(sId, snapshotId);
                        }
                        returnStorage.add(temp);
                    }
                    else if(entry.getValue() instanceof RelationshipProxy)
                    {
                        RelationshipProxy r = (RelationshipProxy) entry.getValue();
                        NodeStorage start = new NodeStorage(r.getStartNode().getLabels().iterator().next().name(), r.getStartNode().getAllProperties());
                        NodeStorage end = new NodeStorage(r.getEndNode().getLabels().iterator().next().name(), r.getEndNode().getAllProperties());

                        RelationshipStorage temp = new RelationshipStorage(r.getType().name(), r.getAllProperties(), start, end);
                        returnStorage.add(temp);
                        if(temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            Object sId =  temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            OutDatedDataException.checkSnapshotId(sId, snapshotId);
                        }
                    }
                }
            }

            tx.success();
        }
        return returnStorage;
    }

    /**
     * Creates a complete Neo4j cypher String for a certain relationshipStorage
     * @param relationshipStorage the relationshipStorage to transform.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildRelationshipString(final RelationshipStorage relationshipStorage)
    {
        return buildNodeString(relationshipStorage.getStartNode(), "1") + buildPureRelationshipString(relationshipStorage) +
                buildNodeString(relationshipStorage.getEndNode(), "2");
    }

    /**
     * Creates a Neo4j cypher String for a certain relationshipStorage
     * @param relationshipStorage the relationshipStorage to transform.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildPureRelationshipString(final RelationshipStorage relationshipStorage)
    {
        StringBuilder builder = new StringBuilder();

        builder.append("-[r");

        if (!relationshipStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s", relationshipStorage.getId()));
        }


        if(!relationshipStorage.getProperties().isEmpty())
        {
            builder.append(" {");
            Iterator<Map.Entry<String, Object>> iterator = relationshipStorage.getProperties().entrySet().iterator();

            while (iterator.hasNext())
            {
                Map.Entry<String, Object> currentProperty = iterator.next();
                builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getValue().toString()));

                if (iterator.hasNext())
                {
                    builder.append(" , ");
                }
            }
            builder.append("}");
        }
        builder.append("]->");

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

        if(!nodeStorage.getProperties().isEmpty())
        {
            builder.append(" {");

            Iterator<Map.Entry<String, Object>> iterator = nodeStorage.getProperties().entrySet().iterator();

            while (iterator.hasNext())
            {
                Map.Entry<String, Object> currentProperty = iterator.next();
                builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getValue().toString()));

                if (iterator.hasNext())
                {
                    builder.append(" , ");
                }
            }
            builder.append("}");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public boolean compareNode(final NodeStorage nodeStorage)
    {
        if (graphDb == null)
        {
            start();
        }

        final String builder = MATCH + buildNodeString(nodeStorage, "") + " RETURN n";
        Result result = graphDb.execute(builder);

        //Assuming we only get one node in return.
        if (result.hasNext())
        {
            Map<String, Object> value = result.next();
            for (Map.Entry<String, Object> entry : value.entrySet())
            {
                if (entry.getValue() instanceof NodeProxy)
                {
                    NodeProxy n = (NodeProxy) entry.getValue();

                    try
                    {
                        return HashCreator.sha1FromNode(nodeStorage).equals(n.getProperty(Constants.TAG_HASH));
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.getLogger().warn("Couldn't execute SHA1 for node", e);
                    }
                    break;
                }
            }
        }

        //If can't find the node its different probably.
        return false;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        try
        {
            final StringBuilder builder = new StringBuilder(MATCH + buildNodeString(key, ""));

            Set<String> keys = key.getProperties().keySet();
            keys.addAll(value.getProperties().keySet());

            for (String tempKey : keys)
            {
                Object value1 = key.getProperties().get(tempKey);
                Object value2 = value.getProperties().get(tempKey);

                if (value1 == null)
                {
                    builder.append(" SET n.").append(tempKey).append(" = ").append(value2);
                }
                else if (value2 == null)
                {
                    builder.append(String.format(" REMOVE n.%s", tempKey));
                }
                else
                {
                    if (value1.equals(value2))
                    {
                        continue;
                    }

                    builder.append(" SET n.").append(tempKey).append(" = ").append(value2);
                }
            }

            NodeStorage tempStorage = new NodeStorage(value.getId(), key.getProperties());
            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                tempStorage.addProperty(entry.getKey(), entry.getValue());
            }

            builder.append(String.format(" SET n.%s = '%s'", Constants.TAG_HASH, HashCreator.sha1FromNode(tempStorage)));
            builder.append(String.format(" SET n.%s = ", Constants.TAG_SNAPSHOT_ID)).append(snapshotId);

            graphDb.execute(builder.toString());
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute update node transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed update node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        try(Transaction tx = graphDb.beginTx())
        {
            final Label label = storage::getId;
            final Node myNode = graphDb.createNode(label);

            for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
            {
                myNode.setProperty(entry.getKey(), entry.getValue());
            }
            myNode.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(storage));
            myNode.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);

            tx.success();
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed create node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        try
        {
            final String cypher = MATCH + buildNodeString(storage, "") + " DETACH DELETE n";
            graphDb.execute(cypher);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete node transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed delete node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        try
        {
            final StringBuilder builder = new StringBuilder(MATCH + buildRelationshipString(key));

            if (!key.getId().equals(value.getId()))
            {
                builder.append(String.format("REMOVE n:%s", key.getId()));
                builder.append(String.format("SET n:%s", value.getId()));
            }

            Set<String> keys = key.getProperties().keySet();
            keys.addAll(value.getProperties().keySet());

            for (String tempKey : keys)
            {
                Object value1 = key.getProperties().get(tempKey);
                Object value2 = value.getProperties().get(tempKey);

                if (value1 == null)
                {
                    builder.append(" SET n.").append(tempKey).append(" = ").append(value2);
                }
                else if (value2 == null)
                {
                    builder.append(String.format("REMOVE n.%s", tempKey));
                }
                else
                {
                    if (value1.equals(value2))
                    {
                        continue;
                    }
                    builder.append(" SET n.").append(tempKey).append(" = ").append(value2);
                }
            }

            RelationshipStorage tempStorage = new RelationshipStorage(value.getId(),
                    key.getProperties(),
                    key.getStartNode(),
                    key.getEndNode());
            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                tempStorage.addProperty(entry.getKey(), entry.getValue());
            }

            builder.append(String.format(" SET n.%s = '%s'", Constants.TAG_HASH, HashCreator.sha1FromRelationship(tempStorage)));
            builder.append(String.format(" SET n.%s = '%s'", Constants.TAG_SNAPSHOT_ID, snapshotId));
            graphDb.execute(builder.toString());
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute update relationship transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed update relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            storage.addProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(storage));
            storage.addProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);

            final String builder = MATCH + buildNodeString(storage.getStartNode(), "1") +
                    ", " +
                    buildNodeString(storage.getEndNode(), "2") +
                    " CREATE (n1)" +
                    buildPureRelationshipString(storage) +
                    "(n2)";
            graphDb.execute(builder);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create relationship transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed create relationship transaction in server:  " + id);

        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            //Delete relationship
            final String cypher = MATCH + buildRelationshipString(storage) + " DELETE r";
            graphDb.execute(cypher);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete relationship transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().warn("Executed delete relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage relationshipStorage)
    {

        final String builder = MATCH + buildRelationshipString(relationshipStorage) + " RETURN r";
        Result result = graphDb.execute(builder);

        //Assuming we only get one node in return.
        if (result.hasNext())
        {
            Map<String, Object> value = result.next();
            for (Map.Entry<String, Object> entry : value.entrySet())
            {
                if (entry.getValue() instanceof NodeProxy)
                {
                    NodeProxy n = (NodeProxy) entry.getValue();

                    try
                    {
                        return HashCreator.sha1FromRelationship(relationshipStorage).equals(n.getProperty(Constants.TAG_HASH));
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.getLogger().warn("Couldn't execute SHA1 for relationship", e);
                    }
                    break;
                }
            }
        }

        return false;
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
                Log.getLogger().info("Shutting down Neo4j.");
                graphDb.shutdown();
            }
        } );
    }
}
