package main.java.com.bag.server.database;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Class created to handle access to the OrientDB database.
 */
public class OrientDBDatabaseAccess implements IDatabaseAccess
{
    //todo May improve performance by commiting only after whole stack.
    /**
     * The base path of the database.
     */
    private static final String BASE_PATH = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/OrientDB";

    /**
     * The id of the server.
     */
    private final int id;

    /**
     * The orientDb factory object.
     */
    private OrientGraphFactory factory;

    /**
     * Constructor which sets the id of the server already.
     * @param id sets the id.
     */
    public OrientDBDatabaseAccess(final int id)
    {
        this.id = id;
    }

    @Override
    public void start()
    {
        factory = new OrientGraphFactory(BASE_PATH).setupPool(1,10);
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

        if(factory == null)
        {
            start();
        }

        ArrayList<Object> returnStorage =  new ArrayList<>();

        OrientGraph graph = factory.getTx();
        try
        {
            //If nodeStorage is null, we're obviously trying to read relationships.
            if(nodeStorage == null)
            {
                final String relationshipId = relationshipStorage.getId();

                Iterable<Vertex> startNodes = getVertexList(relationshipStorage.getStartNode(), graph);
                Iterable<Vertex> endNodes = getVertexList(relationshipStorage.getEndNode(), graph);

                List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                        .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                        .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                        .collect(Collectors.toList());
                for(Edge edge: list)
                {
                    Object sId = edge.getProperty(Constants.TAG_SNAPSHOT_ID);
                    //todo relationship get by properties not yet supported.
                    if(sId == null || (sId instanceof Long && (long) sId <= snapshotId))
                    {
                        RelationshipStorage storage = new RelationshipStorage(edge.getClass().toString(), relationshipStorage.getStartNode(), relationshipStorage.getEndNode());
                        for (String key : edge.getPropertyKeys())
                        {
                            storage.addProperty(key, edge.getProperty(key));
                        }
                        if(storage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            Object localSId =  storage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            OutDatedDataException.checkSnapshotId(localSId, snapshotId);
                        }
                        returnStorage.add(storage);
                    }
                }
            }
            else
            {
                for (final Vertex tempVertex : getVertexList(nodeStorage, graph))
                {
                    Object sId = tempVertex.getProperty(Constants.TAG_SNAPSHOT_ID);

                    if(sId == null || (sId instanceof Long && (long) sId <= snapshotId))
                    {
                        NodeStorage temp = new NodeStorage(tempVertex.getClass().toString());
                        for (String key : tempVertex.getPropertyKeys())
                        {
                            temp.addProperty(key, tempVertex.getProperty(key));
                        }
                        if(temp.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            Object localSId =  temp.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            OutDatedDataException.checkSnapshotId(localSId, snapshotId);
                        }
                        returnStorage.add(temp);
                    }
                }
            }

            //odb.createVertexType("Person"); this is our class (neo4j label)

        }
        finally
        {
            graph.shutdown();
        }

        return returnStorage;
    }

    /**
     * Returns a list of vertices from the database matching the nodeStorage.
     * @param nodeStorage the nodeStorage.
     * @param graph the graph database.
     * @return a list of vertices.
     */
    private Iterable<Vertex> getVertexList(final NodeStorage nodeStorage, final OrientGraph graph)
    {
        String[] propertyKeys   = nodeStorage.getProperties().keySet().toArray(new String[0]);
        Object[] propertyValues = nodeStorage.getProperties().values().toArray();
        return graph.getVertices(nodeStorage.getId(), propertyKeys , propertyValues);
    }

    /**
     * Kills the graph database.
     */
    @Override
    public void terminate()
    {
        factory.close();
    }


    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param nodeStorage the node to compare
     * @return true if equal hash, else false.
     */
    @Override
    public boolean compareNode(final NodeStorage nodeStorage)
    {
        if(factory == null)
        {
            start();
        }

        OrientGraph graph = factory.getTx();
        try
        {
            //Assuming we only get one node in return.
            for (final Vertex tempVertex : getVertexList(nodeStorage, graph))
            {
                if(!HashCreator.sha1FromNode(nodeStorage).equals(tempVertex.getProperty("hash")))
                {
                    return false;
                }
            }
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Failed at generating hash in server " + id, e);
        }
        finally
        {
            graph.shutdown();
        }

        return true;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            Iterable<Vertex> result = getVertexList(key, graph);

            Set<String> keys = key.getProperties().keySet();
            keys.addAll(value.getProperties().keySet());

            NodeStorage tempStorage = new NodeStorage(value.getId(), key.getProperties());
            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                tempStorage.addProperty(entry.getKey(), entry.getValue());
            }

            for (Vertex vertex : result)
            {
                for (String tempKey : keys)
                {
                    Object value1 = key.getProperties().get(tempKey);
                    Object value2 = value.getProperties().get(tempKey);

                    if (value1 == null)
                    {
                        vertex.setProperty(tempKey, value2);
                    }
                    else if (value2 == null)
                    {
                        vertex.removeProperty(tempKey);
                    }
                    else
                    {
                        if (value1.equals(value2))
                        {
                            continue;
                        }
                        vertex.setProperty(tempKey, value2);
                    }
                }
                vertex.setProperty(Constants.TAG_HASH, tempStorage);
                vertex.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
            }

            graph.commit();
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute update node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            Vertex vertex = graph.addVertex(storage.getId());
            for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
            {
                vertex.setProperty(entry.getKey(), entry.getValue());
            }
            vertex.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(storage));
            vertex.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);

            graph.commit();
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            for (final Vertex vertex : getVertexList(storage, graph))
            {
                vertex.remove();
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            final String relationshipId = key.getId();

            Iterable<Vertex> startNodes = getVertexList(key.getStartNode(), graph);
            Iterable<Vertex> endNodes = getVertexList(key.getEndNode(), graph);

            NodeStorage tempStorage = new NodeStorage(value.getId(), key.getProperties());
            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                tempStorage.addProperty(entry.getKey(), entry.getValue());
            }

            Set<String> keys = key.getProperties().keySet();
            keys.addAll(value.getProperties().keySet());

            List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                    .collect(Collectors.toList());
            for (Edge edge : list)
            {
                for (String tempKey : keys)
                {
                    Object value1 = key.getProperties().get(tempKey);
                    Object value2 = value.getProperties().get(tempKey);

                    if (value1 == null)
                    {
                        edge.setProperty(tempKey, value2);
                    }
                    else if (value2 == null)
                    {
                        edge.removeProperty(tempKey);
                    }
                    else
                    {
                        if (value1.equals(value2))
                        {
                            continue;
                        }
                        edge.setProperty(tempKey, value2);
                    }
                }
                edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(tempStorage));
                edge.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute update relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            Iterable<Vertex> startNodes = this.getVertexList(storage.getStartNode(), graph);
            Iterable<Vertex> endNodes = this.getVertexList(storage.getEndNode(), graph);

            for (Vertex startNode : startNodes)
            {
                for (Vertex endNode : endNodes)
                {
                    Edge edge = startNode.addEdge(storage.getId(), endNode);

                    for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
                    {
                        edge.setProperty(entry.getKey(), entry.getValue());
                    }
                    edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(storage));
                    edge.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                }
            }

            graph.commit();
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            final String relationshipId = storage.getId();

            Iterable<Vertex> startNodes = getVertexList(storage.getStartNode(), graph);
            Iterable<Vertex> endNodes = getVertexList(storage.getEndNode(), graph);

            StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                    .forEach(Edge::remove);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.shutdown();
        }
        return true;
    }

    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param relationshipStorage the node to compare
     * @return true if equal hash, else false.
     */
    public boolean compareRelationship(final RelationshipStorage relationshipStorage)
    {
        if (factory == null)
        {
            start();
        }

        OrientGraph graph = factory.getTx();
        try
        {
            final String relationshipId = relationshipStorage.getId();

            Iterable<Vertex> startNodes = getVertexList(relationshipStorage.getStartNode(), graph);
            Iterable<Vertex> endNodes = getVertexList(relationshipStorage.getEndNode(), graph);

            List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                    .collect(Collectors.toList());
            for (Edge edge : list)
            {
                if(!HashCreator.sha1FromRelationship(relationshipStorage).equals(edge.getProperty("hash")))
                {
                    return false;
                }
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Failed at generating hash in server " + id, e);
        }
        finally
        {
            graph.shutdown();
        }

        return true;
    }
}
