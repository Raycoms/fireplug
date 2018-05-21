package main.java.com.bag.database;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static main.java.com.bag.util.Constants.TAG_PRE;
import static main.java.com.bag.util.Constants.TAG_SNAPSHOT_ID;
import static main.java.com.bag.util.Constants.TAG_VERSION;

/**
 * Class created to handle access to the neo4j database.
 */
public class Neo4jDatabaseAccess implements IDatabaseAccess
{
    /**
     * Base path location of the Neo4j database.
     */
    private static final String BASE_PATH = System.getProperty("user.home") + "/Neo4jDB";

    /**
     * String used to match key value pairs.
     */
    private static final String KEY_VALUE_PAIR = "%s: {%s}";

    /**
     * String used to match keys.
     */
    private static final String MATCH = "MATCH ";

    /**
     * If the DB runs in multi-version mode.
     */
    private final boolean multiVersion;

    /**
     * The graphDB object.
     */
    private GraphDatabaseService graphDb;

    /**
     * Id of the database. (If multiple running on the same machine.
     */
    private final int id;

    /**
     * If we're running a direct access client, has Neo4j's database address
     */
    private final String haAddresses;

    /**
     * Pool for kryo objects.
     */
    @Nullable
    private KryoPool pool;

    /**
     * Public constructor.
     * @param id, id of the server.
     * @param pool the kryo factory.
     */
    public Neo4jDatabaseAccess(final int id, final String haAddresses, final boolean multiVersion, final @Nullable KryoFactory pool)
    {
        this.id = id;
        this.haAddresses = haAddresses;
        this.multiVersion = multiVersion;

        this.pool = pool == null ? null : new KryoPool.Builder(pool).softReferences().build();
    }

    @Override
    public void setPool(final KryoFactory pool)
    {
        this.pool = new KryoPool.Builder(pool).softReferences().build();
    }

    @Override
    public void start()
    {
        Log.getLogger().error("Starting neo4j database service on " + id);
        Log.getLogger().error("Starting neo4j database with multiVersion " + multiVersion);

        if (haAddresses == null)
        {
            final File dbPath = new File(BASE_PATH + id);

            Log.getLogger().warn("Starting Neo4j not HA.");
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).newGraphDatabase();
            registerShutdownHook(graphDb);
            try (Transaction tx = graphDb.beginTx())
            {
                graphDb.execute("CREATE INDEX ON :Node(idx)");
                tx.success();
            }
        }
        else
        {
            final File dbPath = new File(BASE_PATH + (id-1));

            Log.getLogger().setLevel(Level.ALL);
            Log.getLogger().error("Turning on Neo4j HA with path: " + dbPath.toString());
            final GraphDatabaseBuilder builder = new HighlyAvailableGraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath);
            final String[] addresses = haAddresses.split(" ");
            Log.getLogger().error("Addresses: " + Arrays.toString(addresses));
            final  List<String> initialHosts = new ArrayList<>();
            final List<String> servers = new ArrayList<>();
            for (int i = 0; i < addresses.length; i++)
            {
                initialHosts.add(String.format("%s:5001", addresses[i]));
                servers.add(String.format("%s:6001", addresses[i]));
            }
            Log.getLogger().error("initialHosts: " + Arrays.toString(initialHosts.toArray()));
            Log.getLogger().error("servers: " + Arrays.toString(servers.toArray()));
            Log.getLogger().error("Id: " + id);
            Log.getLogger().error("Intitial host: " + String.join(",", initialHosts));
            Log.getLogger().error("HaServer: " + servers.get(0));

            if (id == 1)
            {
                builder.setConfig(HaSettings.slave_only, Settings.FALSE);
            }
            else
            {
                builder.setConfig(HaSettings.slave_only, Settings.TRUE);
            }
            Log.getLogger().error("clusterServer: " + initialHosts.get(id-1));

            builder.setConfig(ClusterSettings.server_id, Integer.toString(id)); //This is correct
            builder.setConfig(ClusterSettings.initial_hosts, String.join(",", initialHosts)); // This is correct
            builder.setConfig(HaSettings.ha_server, servers.get(id-1));
            builder.setConfig(ClusterSettings.cluster_server, initialHosts.get(id-1));
            graphDb = builder.newGraphDatabase();
            Log.getLogger().warn("Finished setup trying empty transaction.");
            registerShutdownHook(graphDb);

            if (id == 1)
            {
                try (Transaction tx = graphDb.beginTx())
                {
                    graphDb.execute("CREATE INDEX ON :Node(idx)");
                    tx.success();
                }
            }
            Log.getLogger().error("HA neo4j database started " + id);

            if (id > 0)
            {
                new Timer().scheduleAtFixedRate(new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        Log.getLogger().error("Ping...");

                    }
                }, 0, 60000);
            }
        }
    }

    @Override
    public void terminate()
    {
        Log.getLogger().info("Shutting down Neo4j manually");
        graphDb.shutdown();
    }

    /**
     * Creates a transaction which will get a list of nodes.
     *
     * @param identifier the nodes which should be retrieved.
     * @param clientId
     * @return the result nodes as a List of NodeStorages..
     */
    @NotNull
    public List<Object> readObject(@NotNull final Object identifier, final long snapshotId, final int clientId) throws OutDatedDataException
    {
        NodeStorage nodeStorage = null;
        RelationshipStorage relationshipStorage = null;

        if (identifier instanceof NodeStorage)
        {
            nodeStorage = (NodeStorage) identifier;
        }
        else if (identifier instanceof RelationshipStorage)
        {
            relationshipStorage = (RelationshipStorage) identifier;
        }
        else
        {
            Log.getLogger().error("Can't read data on object: " + identifier.getClass().toString());
            return Collections.emptyList();
        }

        if (graphDb == null)
        {
            start();
        }

        //We only support 1 label each node/vertex because of compatibility with our graph dbs.
        final ArrayList<Object> returnStorage = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx())
        {
            final Kryo kryo;
            if (multiVersion)
            {
                kryo = pool.borrow();
            }
            else
            {
                kryo = null;
            }

            final StringBuilder builder = new StringBuilder(MATCH);
            final Map<String, Object> properties;

            if (nodeStorage == null)
            {
                Log.getLogger().info(Long.toString(snapshotId));
                builder.append(buildRelationshipString(relationshipStorage));
                builder.append(" RETURN r");
                Log.getLogger().info(builder.toString());

                //Contains params of relationshipStorage.
                properties = transFormToPropertyMap(relationshipStorage.getProperties(), "");

                //Adds also params of start and end node.
                properties.putAll(transFormToPropertyMap(relationshipStorage.getStartNode().getProperties(), "1"));
                properties.putAll(transFormToPropertyMap(relationshipStorage.getEndNode().getProperties(), "2"));
            }
            else
            {
                Log.getLogger().info(Long.toString(snapshotId));
                builder.append(buildNodeString(nodeStorage, ""));
                builder.append(" RETURN n");
                Log.getLogger().info(builder.toString());
                //Converts the keys to upper case to fit the params we send to neo4j.
                properties = transFormToPropertyMap(nodeStorage.getProperties(), "");
            }

            Log.getLogger().info("To database: " + builder.toString());

            final Result result = graphDb.execute(builder.toString(), properties);
            while (result.hasNext())
            {
                final Map<String, Object> value = result.next();

                for (final Map.Entry<String, Object> entry : value.entrySet())
                {
                    if (entry.getValue() instanceof NodeProxy)
                    {
                        final NodeProxy n = (NodeProxy) entry.getValue();
                        NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                        if (temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            final Object sId = temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            if (multiVersion)
                            {
                                temp = OutDatedDataException.getCorrectNodeStorage(sId, snapshotId, temp, kryo);
                            }
                            else
                            {
                                OutDatedDataException.checkSnapshotId(sId, snapshotId);
                            }
                        }
                        temp.removeProperty(Constants.TAG_HASH);

                        if (multiVersion)
                        {
                           // If the version int is < 0, it means it is outdated and we don't need it.
                           final Object propPre = temp.getProperty(TAG_PRE);
                           if(propPre instanceof NodeProxy)
                           {
                               temp.removeProperty(TAG_PRE);
                           }
                           final Object propV = temp.getProperty(TAG_VERSION);
                           if(propV instanceof Integer)
                           {
                               if((Integer) propV < 0)
                               {
                                   continue;
                               }
                               temp.removeProperty(TAG_VERSION);
                           }
                        }
                        returnStorage.add(temp);
                    }
                    else if (entry.getValue() instanceof RelationshipProxy)
                    {
                        final RelationshipProxy r = (RelationshipProxy) entry.getValue();
                        final NodeStorage start = new NodeStorage(r.getStartNode().getLabels().iterator().next().name(), r.getStartNode().getAllProperties());
                        final NodeStorage end = new NodeStorage(r.getEndNode().getLabels().iterator().next().name(), r.getEndNode().getAllProperties());

                        RelationshipStorage temp = new RelationshipStorage(r.getType().name(), r.getAllProperties(), start, end);
                        if (temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            final Object sId = temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            if (multiVersion)
                            {
                                temp = OutDatedDataException.getCorrectRSStorage(sId, snapshotId, temp, kryo);
                            }
                            else
                            {
                                OutDatedDataException.checkSnapshotId(sId, snapshotId);
                            }
                        }
                        temp.removeProperty(Constants.TAG_HASH);
                        if (multiVersion)
                        {
                            // If the version int is < 0, it means it is outdated and we don't need it.
                            final Object propPre = temp.getProperty(TAG_PRE);
                            if(propPre instanceof RelationshipProxy)
                            {
                                temp.removeProperty(TAG_PRE);
                            }
                            final Object propV = temp.getProperty(TAG_VERSION);
                            if(propV instanceof Integer)
                            {
                                if((Integer) propV < 0)
                                {
                                    continue;
                                }
                                temp.removeProperty(TAG_VERSION);
                            }
                        }
                        returnStorage.add(temp);
                    }
                }
            }
            if (pool != null)
            {
                pool.release(kryo);
            }
            tx.success();
        }
        return returnStorage;
    }

    @Override
    public boolean shouldFollow(final int sequence)
    {
        return sequence != 9;
    }

    @Override
    public String getName()
    {
        return Constants.NEO4J;
    }

    /**
     * Transforms a map of properties to a map of params for neo4j.
     *
     * @param map the map to transform.
     * @param id  the id to add.
     * @return the transformed map.
     */
    private Map<String, Object> transFormToPropertyMap(final Map<String, Object> map, final String id)
    {
        return map.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toUpperCase() + id,
                        Map.Entry::getValue));
    }

    /**
     * Creates a complete Neo4j cypher String for a certain relationshipStorage
     *
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
     *
     * @param relationshipStorage the relationshipStorage to transform.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildPureRelationshipString(final RelationshipStorage relationshipStorage)
    {
        final StringBuilder builder = new StringBuilder();

        builder.append("-[r");

        if (!relationshipStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s", relationshipStorage.getId()));
        }

        if (!relationshipStorage.getProperties().isEmpty())
        {
            builder.append(" {");
            final Iterator<Map.Entry<String, Object>> iterator = relationshipStorage.getProperties().entrySet().iterator();

            while (iterator.hasNext())
            {
                final Map.Entry<String, Object> currentProperty = iterator.next();
                builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getKey().toUpperCase()));

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
     *
     * @param nodeStorage the nodeStorage to transform.
     * @param n           optional identifier in the query.
     * @return a string which may be sent with cypher to neo4j.
     */
    private String buildNodeString(final NodeStorage nodeStorage, final String n)
    {
        final StringBuilder builder = new StringBuilder("(n").append(n);
        if (!nodeStorage.getId().isEmpty())
        {
            builder.append(String.format(":%s", nodeStorage.getId()));
        }

        if (!nodeStorage.getProperties().isEmpty())
        {
            builder.append(" {");

            final Iterator<Map.Entry<String, Object>> iterator = nodeStorage.getProperties().entrySet().iterator();

            while (iterator.hasNext())
            {
                final Map.Entry<String, Object> currentProperty = iterator.next();
                builder.append(String.format(KEY_VALUE_PAIR, currentProperty.getKey(), currentProperty.getKey().toUpperCase() + n));

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

        try (Transaction tx = graphDb.beginTx())
        {
            final StringBuilder builder = new StringBuilder(MATCH);
            builder.append(buildNodeString(nodeStorage, ""));
            builder.append(" RETURN n");

            //Converts the keys to upper case to fit the params we send to neo4j.
            final Map<String, Object> properties = transFormToPropertyMap(nodeStorage.getProperties(), "");
            Log.getLogger().info(builder.toString());
            final Result result = graphDb.execute(builder.toString(), properties);

            //Assuming we only get one node in return.
            while (result.hasNext())
            {
                Log.getLogger().info("Received result!");
                final Map<String, Object> value = result.next();
                for (final Map.Entry<String, Object> entry : value.entrySet())
                {
                    if (entry.getValue() instanceof NodeProxy)
                    {
                        final NodeProxy n = (NodeProxy) entry.getValue();

                        try
                        {
                            final String newSha = HashCreator.sha1FromNode(nodeStorage);
                            if (newSha.equals(n.getProperty(Constants.TAG_HASH)))
                            {
                                return true;
                            }
                        }
                        catch (final NoSuchAlgorithmException e)
                        {
                            Log.getLogger().error("Couldn't execute SHA1 for node", e);
                        }

                        if (!multiVersion)
                        {
                            return false;
                        }

                        final Kryo kryo = pool.borrow();
                        try
                        {
                            NodeStorage temp = new NodeStorage(n.getLabels().iterator().next().name(), n.getAllProperties());
                            if (temp.getProperties().containsKey(TAG_SNAPSHOT_ID))
                            {
                                final Object sId = temp.getProperties().get(TAG_SNAPSHOT_ID);
                                final Object wantedId = nodeStorage.getProperty(TAG_SNAPSHOT_ID);
                                temp = OutDatedDataException.getCorrectNodeStorage(sId, wantedId instanceof Long ? (long) wantedId : -1, temp, kryo);
                            }
                            return HashCreator.sha1FromNode(nodeStorage).equals(temp.getProperty(Constants.TAG_HASH));
                        }
                        catch (final Exception e)
                        {
                            Log.getLogger().error("Couldn't execute SHA1 for node " + nodeStorage.toString(), e);
                        }
                        if (pool != null)
                        {
                            pool.release(kryo);
                        }

                        break;
                    }
                }

                return false;
            }
            tx.success();
        }

        //If can't find the node its different probably.
        return true;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId, final int clientId)
    {
        final Kryo kryo;
        if (multiVersion)
        {
            kryo = pool.borrow();
        }
        else
        {
            kryo = null;
        }
        try (Transaction tx = graphDb.beginTx())
        {
            final Map<String, Object> tempProperties = transFormToPropertyMap(key.getProperties(), "");
            final Result result = graphDb.execute(MATCH + buildNodeString(key, "") + " RETURN n", tempProperties);

            while (result.hasNext())
            {
                final Map<String, Object> resultValue = result.next();

                for (final Map.Entry<String, Object> entry : resultValue.entrySet())
                {
                    if (entry.getValue() instanceof NodeProxy)
                    {
                        final NodeProxy proxy = (NodeProxy) entry.getValue();
                        if(multiVersion)
                        {
                            final Object obj = proxy.hasProperty(TAG_VERSION) ? proxy.getProperty(TAG_VERSION) : null;
                            final NodeStorage temp = new NodeStorage(proxy.getLabels().iterator().next().name(), proxy.getAllProperties());
                            final Output output = new Output(10000);
                            kryo.writeObject(output, temp);
                            proxy.setProperty(TAG_PRE, output.toBytes());
                            output.close();
                            proxy.setProperty(TAG_VERSION, obj instanceof Integer ? (Integer) obj + 1 : 1);

                        }
                        for (final Map.Entry<String, Object> properties : value.getProperties().entrySet())
                        {
                            proxy.setProperty(properties.getKey(), properties.getValue());
                        }

                        proxy.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(new NodeStorage(proxy.getLabels().iterator().next().name(), proxy.getAllProperties())));
                        proxy.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                    }
                }
            }
            tx.success();
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Couldn't execute update node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            if (pool != null)
            {
                pool.release(kryo);
            }
        }
        Log.getLogger().info("Executed update node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId, final int clientId)
    {
        try (Transaction tx = graphDb.beginTx())
        {
            final Label label = storage::getId;
            final Node myNode = graphDb.createNode(label);

            for (final Map.Entry<String, Object> entry : storage.getProperties().entrySet())
            {
                myNode.setProperty(entry.getKey(), entry.getValue());
            }
            myNode.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(storage));
            myNode.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);

            if (multiVersion)
            {
                myNode.setProperty(TAG_VERSION, -1);
            }

            tx.success();
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().info("Executed create node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId, final int clientId)
    {
        try
        {
            if(multiVersion)
            {
                final NodeStorage value = new NodeStorage(storage);
                value.addProperty(TAG_VERSION, -1);
                return applyUpdate(storage, value, snapshotId, clientId);
            }
            else
            {
                final Map<String, Object> properties = transFormToPropertyMap(storage.getProperties(), "");

                final String cypher = MATCH + buildNodeString(storage, "") + " DETACH DELETE n";
                graphDb.execute(cypher, properties);
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Couldn't execute delete node transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().info("Executed delete node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId, final int clientId)
    {
        final Kryo kryo;
        if (multiVersion)
        {
            kryo = pool.borrow();
        }
        else
        {
            kryo = null;
        }
        try
        {
            //Transform relationship params.
            final Map<String, Object> propertyMap = transFormToPropertyMap(key.getProperties(), "");

            //Adds also params of start and end node.
            propertyMap.putAll(transFormToPropertyMap(key.getStartNode().getProperties(), "1"));
            propertyMap.putAll(transFormToPropertyMap(key.getEndNode().getProperties(), "2"));

            final Result result = graphDb.execute(MATCH + buildRelationshipString(key) + " RETURN r", propertyMap);
            while (result.hasNext())
            {
                final Map<String, Object> relValue = result.next();

                for (final Map.Entry<String, Object> entry : relValue.entrySet())
                {
                    if (entry.getValue() instanceof RelationshipProxy)
                    {
                        final RelationshipProxy proxy = (RelationshipProxy) entry.getValue();
                        if(multiVersion)
                        {

                            final Object obj = proxy.hasProperty(TAG_VERSION) ? proxy.getProperty(TAG_VERSION) : null;
                            final NodeStorage start = new NodeStorage(proxy.getStartNode().getLabels().iterator().next().name(), proxy.getStartNode().getAllProperties());
                            final NodeStorage end = new NodeStorage(proxy.getEndNode().getLabels().iterator().next().name(), proxy.getEndNode().getAllProperties());

                            final RelationshipStorage temp = new RelationshipStorage(proxy.getType().name(), proxy.getAllProperties(), start, end);
                            final Output output = new Output(10000);
                            kryo.writeObject(output, temp);
                            proxy.setProperty(TAG_PRE, output.toBytes());
                            output.clear();
                            output.close();
                            proxy.setProperty(TAG_VERSION, obj instanceof Integer ? (Integer) obj + 1 : 1);
                        }

                        for (final Map.Entry<String, Object> properties : value.getProperties().entrySet())
                        {
                            proxy.setProperty(properties.getKey(), properties.getValue());
                        }

                        final NodeStorage start = new NodeStorage(proxy.getStartNode().getLabels().iterator().next().name(), proxy.getStartNode().getAllProperties());
                        final NodeStorage end = new NodeStorage(proxy.getEndNode().getLabels().iterator().next().name(), proxy.getEndNode().getAllProperties());

                        proxy.setProperty(Constants.TAG_HASH,
                                HashCreator.sha1FromRelationship(new RelationshipStorage(proxy.getType().name(), proxy.getAllProperties(), start, end)));
                        proxy.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                    }
                }
            }
        }
        catch (final NoSuchAlgorithmException e)
        {
            Log.getLogger().error("Couldn't execute update relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            if (pool != null)
            {
                pool.release(kryo);
            }
        }
        Log.getLogger().info("Executed update relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId, final int clientId)
    {
        try
        {
            final RelationshipStorage tempStorage = new RelationshipStorage(storage.getId(), storage.getProperties(), storage.getStartNode(), storage.getEndNode());
            tempStorage.addProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(storage));
            tempStorage.addProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);

            final String builder = MATCH + buildNodeString(tempStorage.getStartNode(), "1") +
                    ", " +
                    buildNodeString(tempStorage.getEndNode(), "2") +
                    " CREATE (n1)" +
                    buildPureRelationshipString(tempStorage) +
                    "(n2)";

            if (multiVersion)
            {
               tempStorage.addProperty(TAG_VERSION,-1);
            }

            //Transform relationship params.
            final Map<String, Object> properties = transFormToPropertyMap(tempStorage.getProperties(), "");

            //Adds also params of start and end node.
            properties.putAll(transFormToPropertyMap(tempStorage.getStartNode().getProperties(), "1"));
            properties.putAll(transFormToPropertyMap(tempStorage.getEndNode().getProperties(), "2"));

            graphDb.execute(builder, properties);
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Couldn't execute create relationship transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().info("Executed create relationship transaction in server:  " + id);

        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId, final int clientId)
    {
        try
        {
            if(multiVersion)
            {
                final RelationshipStorage value = new RelationshipStorage(storage);
                value.addProperty(TAG_VERSION, -1);
                return applyUpdate(storage, value, snapshotId, clientId);
            }
            else
            {
                //Delete relationship
                final String cypher = MATCH + buildRelationshipString(storage) + " DELETE r";

                //Transform relationship params.
                final Map<String, Object> properties = transFormToPropertyMap(storage.getProperties(), "");

                //Adds also params of start and end node.
                properties.putAll(transFormToPropertyMap(storage.getStartNode().getProperties(), "1"));
                properties.putAll(transFormToPropertyMap(storage.getEndNode().getProperties(), "2"));

                graphDb.execute(cypher, properties);
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Couldn't execute delete relationship transaction in server:  " + id, e);
            return false;
        }
        Log.getLogger().info("Executed delete relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean startTransaction()
    {
        return false;
    }

    @Override
    public boolean commitTransaction()
    {
        return false;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage relationshipStorage)
    {
        final String builder = MATCH + buildRelationshipString(relationshipStorage) + " RETURN r";

        //Contains params of relationshipStorage.
        final Map<String, Object> properties = transFormToPropertyMap(relationshipStorage.getProperties(), "");

        //Adds also params of start and end node.
        properties.putAll(transFormToPropertyMap(relationshipStorage.getStartNode().getProperties(), "1"));
        properties.putAll(transFormToPropertyMap(relationshipStorage.getEndNode().getProperties(), "2"));

        final Result result = graphDb.execute(builder, properties);

        //Assuming we only get one node in return.
        if (result.hasNext())
        {
            final Map<String, Object> value = result.next();
            for (final Map.Entry<String, Object> entry : value.entrySet())
            {
                if (entry.getValue() instanceof RelationshipProxy)
                {
                    final RelationshipProxy r = (RelationshipProxy) entry.getValue();
                    try
                    {
                        final String newSha = HashCreator.sha1FromRelationship(relationshipStorage);
                        if (newSha.equals(r.getProperty(Constants.TAG_HASH)))
                        {
                            return true;
                        }
                    }
                    catch (final NoSuchAlgorithmException e)
                    {
                        Log.getLogger().error("Couldn't execute SHA1 for rs", e);
                    }

                    if (!multiVersion)
                    {
                        return false;
                    }

                    final Kryo kryo = pool.borrow();

                    try
                    {
                        final NodeStorage start = new NodeStorage(r.getStartNode().getLabels().iterator().next().name(), r.getStartNode().getAllProperties());
                        final NodeStorage end = new NodeStorage(r.getEndNode().getLabels().iterator().next().name(), r.getEndNode().getAllProperties());

                        RelationshipStorage temp = new RelationshipStorage(r.getType().name(), r.getAllProperties(), start, end);
                        if (temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            final Object sId = temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            final Object snapshotId = relationshipStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            temp = OutDatedDataException.getCorrectRSStorage(sId, snapshotId instanceof Long ? (long) snapshotId : -1, temp, kryo);

                            return HashCreator.sha1FromRelationship(relationshipStorage).equals(temp.getProperty(Constants.TAG_HASH));
                        }
                    }
                    catch (final Exception e)
                    {
                        Log.getLogger().error("Couldn't execute SHA1 for relationship", e);
                    }

                    if (pool != null)
                    {
                        pool.release(kryo);
                    }
                    break;
                }
            }
        }

        return true;
    }

    /**
     * Registers a shutdown hook for the Neo4j instance so that it
     * shuts down nicely when the VM exits (even if you "Ctrl-C" the
     * running application).
     *
     * @param graphDb the graphDB to register the shutDownHook to.
     */
    private static void registerShutdownHook(final GraphDatabaseService graphDb)
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            Log.getLogger().info("Shutting down Neo4j.");
            graphDb.shutdown();
        }));
    }
}
