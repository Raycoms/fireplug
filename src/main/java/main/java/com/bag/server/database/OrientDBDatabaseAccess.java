package main.java.com.bag.server.database;

import com.orientechnologies.orient.core.exception.OQueryParsingException;
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
    /**
     * The base path of the database.
     */
    private static final String BASE_PATH = "PLOCAL:" + System.getProperty("user.home") + "/OrientDB";

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
     *
     * @param id sets the id.
     */
    public OrientDBDatabaseAccess(final int id)
    {
        this.id = id;
    }

    @Override
    public void start()
    {
        Log.getLogger().warn("Starting OrientDB database service on " + this.id);

        factory = new OrientGraphFactory(BASE_PATH + this.id).setupPool(1, 10);
    }

    /**
     * Creates a transaction which will get a list of nodes.
     *
     * @param identifier the nodes which should be retrieved.
     * @return the result nodes as a List of NodeStorages..
     */
    @NotNull
    @Override
    public List<Object> readObject(@NotNull Object identifier, long snapshotId) throws OutDatedDataException
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
            Log.getLogger().warn("Can't read data on object: " + identifier.getClass().toString());
            return Collections.emptyList();
        }

        if (factory == null)
        {
            start();
        }

        final ArrayList<Object> returnStorage = new ArrayList<>();

        final OrientGraph graph = factory.getTx();
        try
        {
            //If nodeStorage is null, we're obviously trying to read relationships.
            if (nodeStorage == null)
            {
                final String relationshipId = "class:" + relationshipStorage.getId();
                final Iterable<Vertex> endNodes = getVertexList(relationshipStorage.getEndNode(), graph);

                final List<Edge> list;
                if (relationshipStorage.getStartNode().getProperties().isEmpty())
                {
                    list = StreamSupport.stream(endNodes.spliterator(), false)
                            .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.IN, relationshipId).spliterator(), false))
                            .collect(Collectors.toList());
                }
                else
                {
                    final Iterable<Vertex> startNodes = getVertexList(relationshipStorage.getStartNode(), graph);
                    list = StreamSupport.stream(endNodes.spliterator(), false)
                            .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.IN, relationshipId).spliterator(), false))
                            .filter(edge -> StreamSupport.stream(startNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                            .collect(Collectors.toList());
                }
                for (final Edge edge : list)
                {
                    final RelationshipStorage tempStorage = getRelationshipStorageFromEdge(edge, snapshotId);
                    returnStorage.add(tempStorage);
                }
            }
            else
            {
                for (final Vertex tempVertex : getVertexList(nodeStorage, graph))
                {
                    final NodeStorage tempStorage = getNodeStorageFromVertex(tempVertex);
                    if (tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                    {
                        final Object localSId = tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                        OutDatedDataException.checkSnapshotId(localSId, snapshotId);
                        tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                    }
                    returnStorage.add(tempStorage);
                }
            }
        }
        finally
        {
            graph.shutdown();
        }

        return returnStorage;
    }

    /**
     * Generated a RelationshipStorage from an Edge.
     *
     * @param edge       the base edge.
     * @param snapshotId the snapshot id.
     * @return the relationshipStorage.
     */
    private RelationshipStorage getRelationshipStorageFromEdge(Edge edge, long snapshotId) throws OutDatedDataException
    {
        RelationshipStorage tempStorage =
                new RelationshipStorage(edge.getLabel(), getNodeStorageFromVertex(edge.getVertex(Direction.OUT)), getNodeStorageFromVertex(edge.getVertex(Direction.IN)));
        for (String key : edge.getPropertyKeys())
        {
            if (key.equals(Constants.TAG_SNAPSHOT_ID))
            {
                Object localSId = tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                OutDatedDataException.checkSnapshotId(localSId, snapshotId);
                tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
            }
            tempStorage.addProperty(key, edge.getProperty(key));
        }
        return tempStorage;
    }

    /**
     * Generated a NodeStorage from a Vertex.
     *
     * @param tempVertex the base vertex.
     * @return the nodeStorage.
     */
    private NodeStorage getNodeStorageFromVertex(Vertex tempVertex)
    {
        final NodeStorage temp = new NodeStorage(tempVertex.getProperty("idx"));
        for (final String key : tempVertex.getPropertyKeys())
        {
            temp.addProperty(key, tempVertex.getProperty(key));
        }
        return temp;
    }

    /**
     * Returns a list of vertices from the database matching the nodeStorage.
     *
     * @param nodeStorage the nodeStorage.
     * @param graph       the graph database.
     * @return a list of vertices.
     */
    private Iterable<Vertex> getVertexList(final NodeStorage nodeStorage, final OrientGraph graph)
    {
        final String[] propertyKeys = nodeStorage.getProperties().keySet().toArray(new String[0]);
        final Object[] propertyValues = nodeStorage.getProperties().values().toArray();

        try
        {
            return graph.getVertices(nodeStorage.getId(), propertyKeys, propertyValues);
        }
        catch (OQueryParsingException e)
        {
            Log.getLogger().info(String.format("Class %s doesn't exist.", nodeStorage.getId()), e);
            return Collections.emptyList();
        }
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
     *
     * @param nodeStorage the node to compare
     * @return true if equal hash, else false.
     */
    @Override
    public boolean compareNode(final NodeStorage nodeStorage)
    {
        if (factory == null)
        {
            start();
        }

        final OrientGraph graph = factory.getTx();
        try
        {
            //Assuming we only get one node in return.
            for (final Vertex tempVertex : getVertexList(nodeStorage, graph))
            {
                return HashCreator.sha1FromNode(nodeStorage).equals(tempVertex.getProperty("hash"));
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

        return false;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        final OrientGraph graph = factory.getTx();
        try
        {
            final Iterable<Vertex> result = getVertexList(key, graph);

            for (final Vertex vertex : result)
            {
                for (final Map.Entry<String, Object> entry : value.getProperties().entrySet())
                {
                    vertex.setProperty(entry.getKey(), entry.getValue());
                }

                vertex.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(getNodeStorageFromVertex(vertex)));
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
        final OrientGraph graph = factory.getTx();
        try
        {
            if (graph.getVertexType(storage.getId()) == null)
            {
                graph.createVertexType(storage.getId());
                graph.createKeyIndex("idx", Vertex.class);
            }
            final Vertex vertex = graph.addVertex("class:" + storage.getId(), storage.getProperties());
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
        Log.getLogger().info("Successfully executed create node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        final OrientGraph graph = factory.getTx();
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
            final String relationshipId = "class:" + key.getId();

            final Iterable<Vertex> startNodes = getVertexList(key.getStartNode(), graph);
            final Iterable<Vertex> endNodes = getVertexList(key.getEndNode(), graph);

            final List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.IN).equals(vertex)))
                    .collect(Collectors.toList());
            for (final Edge edge : list)
            {
                for (final Map.Entry<String, Object> entry : value.getProperties().entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(getRelationshipStorageFromEdge(edge, snapshotId)));
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
        if (factory.getNoTx().getEdgeType(storage.getId()) == null)
        {
            factory.getNoTx().createEdgeType(storage.getId());
        }

        final OrientGraph graph = factory.getTx();
        try
        {
            final Iterable<Vertex> startNodes = this.getVertexList(storage.getStartNode(), graph);
            final Iterable<Vertex> endNodes = this.getVertexList(storage.getEndNode(), graph);

            for (final Vertex startNode : startNodes)
            {
                for (final Vertex endNode : endNodes)
                {
                    final String edgeClass = "class:" + storage.getId();
                    final Edge edge = startNode.addEdge(edgeClass, endNode);

                    for (final Map.Entry<String, Object> entry : storage.getProperties().entrySet())
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
        Log.getLogger().info("Successfully executed create relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            final String relationshipId = "class:" + storage.getId();

            Iterable<Vertex> startNodes = getVertexList(storage.getStartNode(), graph);
            Iterable<Vertex> endNodes = getVertexList(storage.getEndNode(), graph);

            StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.IN).equals(vertex)))
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
     *
     * @param relationshipStorage the node to compare
     * @return true if equal hash, else false.
     */
    @Override
    public boolean compareRelationship(final RelationshipStorage relationshipStorage)
    {
        if (factory == null)
        {
            start();
        }

        OrientGraph graph = factory.getTx();
        try
        {
            final String relationshipId = "class:" + relationshipStorage.getId();

            final Iterable<Vertex> startNodes = getVertexList(relationshipStorage.getStartNode(), graph);
            final Iterable<Vertex> endNodes = getVertexList(relationshipStorage.getEndNode(), graph);

            final List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.IN).equals(vertex)))
                    .collect(Collectors.toList());
            for (final Edge edge : list)
            {
                return HashCreator.sha1FromRelationship(relationshipStorage).equals(edge.getProperty(Constants.TAG_HASH));
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

        return false;
    }
}
