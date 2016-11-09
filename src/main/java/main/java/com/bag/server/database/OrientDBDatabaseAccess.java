package main.java.com.bag.server.database;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import org.jetbrains.annotations.NotNull;
import scala.collection.immutable.Stream;

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
    public List<Object> readObject(@NotNull Object identifier, long snapshotId)
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
        factory.close();;
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
            equalHashRelationship(readSet);
        }

        return true;
    }

    /**
     * Checks if the hash of a node is equal to the hash in the database.
     * @param readSet the readSet of nodes which should be compared.
     * @return true if all nodes are equal.
     */
    private boolean equalHashNode(final List readSet)
    {
        for(Object storage: readSet)
        {
            if(storage instanceof NodeStorage)
            {
                NodeStorage nodeStorage = (NodeStorage) storage;

                if(!compareNode(nodeStorage))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks if the hash of a list of relationships matches the relationship in the database.
     * @param readSet the set of relationships
     * @return true if all are correct.
     */
    private boolean equalHashRelationship(final List<RelationshipStorage> readSet)
    {
        for(Object storage: readSet)
        {
            if(storage instanceof RelationshipStorage)
            {
                RelationshipStorage relationshipStorage = (RelationshipStorage) storage;

                if(!compareRelationship(relationshipStorage))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param nodeStorage the node to compare
     * @return true if equal hash, else false.
     */
    private boolean compareNode(final NodeStorage nodeStorage)
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

    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param relationshipStorage the node to compare
     * @return true if equal hash, else false.
     */
    private boolean compareRelationship(final RelationshipStorage relationshipStorage)
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

    //todo create hash over incoming storage or do I have to retrieve the whole node data first?
    @Override
    public void execute(
            final List<NodeStorage> createSetNode,
            final List<RelationshipStorage> createSetRelationship,
            final Map<NodeStorage, NodeStorage> updateSetNode,
            final Map<RelationshipStorage, RelationshipStorage> updateSetRelationship,
            final List<NodeStorage> deleteSetNode,
            final List<RelationshipStorage> deleteSetRelationship, long snapshotId)
    {
        if (factory == null)
        {
            start();
        }

        OrientGraph graph = factory.getTx();
        try
        {
            //Create node
            for (NodeStorage node : createSetNode)
            {
                Vertex vertex = graph.addVertex(node.getId());
                for (Map.Entry<String, Object> entry : node.getProperties().entrySet())
                {
                    vertex.setProperty(entry.getKey(), entry.getValue());
                }
                vertex.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(node));
                vertex.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);


                graph.commit();
            }

            //Create relationships
            for (RelationshipStorage relationship : createSetRelationship)
            {
                Iterable<Vertex> startNodes = this.getVertexList(relationship.getStartNode(), graph);
                Iterable<Vertex> endNodes = this.getVertexList(relationship.getEndNode(), graph);

                for(Vertex startNode: startNodes)
                {
                    for(Vertex endNode: endNodes)
                    {
                        Edge edge = startNode.addEdge(relationship.getId(), endNode);

                        for(Map.Entry<String, Object> entry: relationship.getProperties().entrySet())
                        {
                            edge.setProperty(entry.getKey(), entry.getValue());
                        }
                        edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromRelationship(relationship));
                        edge.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                    }
                }

                graph.commit();
            }

            //Update nodes
            for (Map.Entry<NodeStorage, NodeStorage> node : updateSetNode.entrySet())
            {
                Iterable<Vertex> result = getVertexList(node.getKey(), graph);

                Set<String> keys = node.getKey().getProperties().keySet();
                keys.addAll(node.getValue().getProperties().keySet());

                NodeStorage tempStorage = new NodeStorage(node.getValue().getId(), node.getKey().getProperties());
                for(Map.Entry<String, Object> entry: node.getValue().getProperties().entrySet())
                {
                    tempStorage.addProperty(entry.getKey(), entry.getValue());
                }

                for(Vertex vertex: result)
                {
                    for (String key : keys)
                    {
                        Object value1 = node.getKey().getProperties().get(key);
                        Object value2 = node.getValue().getProperties().get(key);

                        if (value1 == null)
                        {
                            vertex.setProperty(key, value2);
                        }
                        else if (value2 == null)
                        {
                            vertex.removeProperty(key);
                        }
                        else
                        {
                            if (value1.equals(value2))
                            {
                                continue;
                            }
                            vertex.setProperty(key, value2);
                        }
                    }
                    vertex.setProperty(Constants.TAG_HASH, tempStorage);
                    vertex.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                }

                graph.commit();
            }

            //Update relationships
            for (Map.Entry<RelationshipStorage, RelationshipStorage> relationship : updateSetRelationship.entrySet())
            {
                final String relationshipId = relationship.getKey().getId();

                Iterable<Vertex> startNodes = getVertexList(relationship.getKey().getStartNode(), graph);
                Iterable<Vertex> endNodes = getVertexList(relationship.getKey().getEndNode(), graph);

                NodeStorage tempStorage = new NodeStorage(relationship.getValue().getId(), relationship.getKey().getProperties());
                for(Map.Entry<String, Object> entry: relationship.getValue().getProperties().entrySet())
                {
                    tempStorage.addProperty(entry.getKey(), entry.getValue());
                }

                Set<String> keys = relationship.getKey().getProperties().keySet();
                keys.addAll(relationship.getValue().getProperties().keySet());

                List<Edge> list = StreamSupport.stream(startNodes.spliterator(), false)
                        .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                        .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                        .collect(Collectors.toList());
                for(Edge edge: list)
                {
                    for(String key : keys)
                    {
                        Object value1 = relationship.getKey().getProperties().get(key);
                        Object value2 = relationship.getValue().getProperties().get(key);

                        if(value1 == null)
                        {
                            edge.setProperty(key, value2);
                        }
                        else if(value2 == null)
                        {
                            edge.removeProperty(key);
                        }
                        else
                        {
                            if(value1.equals(value2))
                            {
                                continue;
                            }
                            edge.setProperty(key, value2);
                        }
                    }
                    edge.setProperty(Constants.TAG_HASH, HashCreator.sha1FromNode(tempStorage));
                    edge.setProperty(Constants.TAG_SNAPSHOT_ID, snapshotId);
                }

            }

            //Delete relationships
            for (RelationshipStorage relationship : deleteSetRelationship)
            {
                final String relationshipId = relationship.getId();

                Iterable<Vertex> startNodes = getVertexList(relationship.getStartNode(), graph);
                Iterable<Vertex> endNodes = getVertexList(relationship.getEndNode(), graph);

                StreamSupport.stream(startNodes.spliterator(), false)
                        .flatMap(vertex1 -> StreamSupport.stream(vertex1.getEdges(Direction.OUT, relationshipId).spliterator(), false))
                        .filter(edge -> StreamSupport.stream(endNodes.spliterator(), false).anyMatch(vertex -> edge.getVertex(Direction.OUT).equals(vertex)))
                        .forEach(Edge::remove);
            }

            for (NodeStorage node : deleteSetNode)
            {

                for (final Vertex vertex : getVertexList(node, graph))
                {
                    vertex.remove();
                }
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't create hash in server " + id, e);
        }
        finally
        {
            graph.shutdown();
        }
    }
}
