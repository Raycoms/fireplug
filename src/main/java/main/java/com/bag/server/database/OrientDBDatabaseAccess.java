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
    /**
     * The base path of the database.
     */
    private static final String BASE_PATH = "PLOCAL:../home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/OrientDB";

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
                    RelationshipStorage tempStorage = getRelationshipStorageFromEdge(edge);
                    if (tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                    {
                        Object localSId = tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                        OutDatedDataException.checkSnapshotId(localSId, snapshotId);
                        tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                    }
                    returnStorage.add(tempStorage);
                }
            }
            else
            {
                for (final Vertex tempVertex : getVertexList(nodeStorage, graph))
                {
                    NodeStorage tempStorage = getNodeStorageFromVertex(tempVertex);
                    if(tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                    {
                        Object localSId =  tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
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
     * @param edge the base edge.
     * @return the relationshipStorage.
     */
    private RelationshipStorage getRelationshipStorageFromEdge(Edge edge)
    {
        RelationshipStorage tempStorage = new RelationshipStorage(edge.getLabel(), getNodeStorageFromVertex(edge.getVertex(Direction.OUT)), getNodeStorageFromVertex(edge.getVertex(Direction.IN)));
        for (String key : edge.getPropertyKeys())
        {
            tempStorage.addProperty(key, edge.getProperty(key));
        }
        return tempStorage;
    }

    /**
     * Generated a NodeStorage from a Vertex.
     * @param tempVertex the base vertex.
     * @return the nodeStorage.
     */
    private NodeStorage getNodeStorageFromVertex(Vertex tempVertex)
    {
        NodeStorage temp = new NodeStorage(tempVertex.getProperty("@class"));
        for (String key : tempVertex.getPropertyKeys())
        {
            temp.addProperty(key, tempVertex.getProperty(key));
        }
        return temp;
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
                return HashCreator.sha1FromNode(nodeStorage).equals(tempVertex.getProperty("hash"));
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

        return false;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        OrientGraph graph = factory.getTx();
        try
        {
            Iterable<Vertex> result = getVertexList(key, graph);

            for (Vertex vertex : result)
            {
                for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
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
        OrientGraph graph = factory.getTx();
        try
        {
            String vertexClass = "class:" + storage.getId();
            Vertex vertex = graph.addVertex(vertexClass);
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
        Log.getLogger().warn("Successfully executed create node transaction in server:  " + id);

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

            List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                    .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                    .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                    .collect(Collectors.toList());
            for (Edge edge : list)
            {
                for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
                {
                    edge.setProperty(entry.getKey(), entry.getValue());
                }
                edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(getRelationshipStorageFromEdge(edge)));
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
                    String edgeClass = "class:" + storage.getId();
                    Edge edge = startNode.addEdge(edgeClass, endNode);

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
