package main.java.com.bag.server.database;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.util.*;


/**
 * Class created to handle access to the titan database.
 */
public class TitanDatabaseAccess implements IDatabaseAccess
{
    private static final String INDEX_NAME = "search";

    private static final String DIRECTORY ="";

    private TitanGraph graph;

    private final int id;

    public TitanDatabaseAccess(int id)
    {
        this.id = id;
    }

    public void start()
    {
        TitanFactory.Builder config = TitanFactory.build();

        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", DIRECTORY);
        config.set("index." + INDEX_NAME + ".backend", "elasticsearch");
        config.set("index." + INDEX_NAME + ".DIRECTORY", DIRECTORY + File.separator + "es");
        config.set("index." + INDEX_NAME + ".elasticsearch.local-mode", true);
        config.set("index." + INDEX_NAME + ".elasticsearch.client-only", false);

        graph = config.open();
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

        if(graph == null)
        {
            start();
        }

        ArrayList<Object> returnStorage =  new ArrayList<>();

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            //If nodeStorage is null, we're obviously trying to read relationships.
            if(nodeStorage == null)
            {
                returnStorage.add(getRelationshipStorages(relationshipStorage, g, snapshotId));
            }
            else
            {
                returnStorage.addAll(getNodeStorages(nodeStorage, g, snapshotId));
            }
        }
        finally
        {
            graph.tx().commit();
        }


        return returnStorage;
    }

    /**
     * Creates a relationShipStorage list by obtaining the info from the graph using.
     * @param relationshipStorage the keys to retrieve from the graph.
     * @param g the graph to retrieve them from.
     * @return a list matching the keys
     */
    private List<RelationshipStorage> getRelationshipStorages(final RelationshipStorage relationshipStorage, final GraphTraversalSource g, long snapshotId)
            throws OutDatedDataException
    {
        ArrayList<Edge> relationshipList =  new ArrayList<>();
        //g.V(1).bothE().where(otherV().hasId(2)).hasLabel('knows').has('weight',gt(0.0))

        ArrayList<Vertex> nodeStartList =  getVertexList(relationshipStorage.getStartNode(), g, snapshotId);
        ArrayList<Vertex> nodeEndList =  getVertexList(relationshipStorage.getEndNode(), g, snapshotId);

        GraphTraversal<Vertex, Edge> tempOutput = g.V(nodeStartList.toArray()).bothE().where(__.is(P.within(nodeEndList.toArray()))).hasLabel(relationshipStorage.getId());


        for (Map.Entry<String, Object> entry : relationshipStorage.getProperties().entrySet())
        {
            if (tempOutput == null)
            {
                break;
            }
            tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
        }

        if(tempOutput != null)
        {
            if(tempOutput.has(Constants.TAG_SNAPSHOT_ID) == null || (tempOutput = tempOutput.has(Constants.TAG_SNAPSHOT_ID, P.lte(snapshotId))) != null)
            {
                tempOutput.fill(relationshipList);
            }
        }

        ArrayList<RelationshipStorage> returnList = new ArrayList<>();

        for(Edge edge: relationshipList)
        {
            RelationshipStorage tempStorage = new RelationshipStorage(edge.label(), relationshipStorage.getStartNode(), relationshipStorage.getEndNode());

            for(String s: edge.keys())
            {
                tempStorage.addProperty(s, edge.property(s));
            }
            if(tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
            {
                Object sId =  tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                OutDatedDataException.checkSnapshotId(sId, snapshotId);
            }

            returnList.add(tempStorage);
        }
        return returnList;
    }

    /**
     * Creates a list of vertices matching a certain nodeStorage.
     * @param nodeStorage the key.
     * @param g the graph.
     * @return the list of vertices.
     */
    private ArrayList<Vertex> getVertexList(final NodeStorage nodeStorage, final GraphTraversalSource g, long snapshotId)
    {
        GraphTraversal<Vertex, Vertex> tempOutput = getVertexList(nodeStorage, g);
        ArrayList<Vertex> nodeList = new ArrayList<>();
        if (tempOutput != null && tempOutput.has(Constants.TAG_SNAPSHOT_ID) == null || (tempOutput = tempOutput.has(Constants.TAG_SNAPSHOT_ID, P.lte(snapshotId))) != null)
        {
            tempOutput.fill(nodeList);
        }

        return nodeList;
    }

    /**
     * Creates a list of vertices matching a certain nodeStorage.
     * @param nodeStorage the key.
     * @param g the graph.
     * @return the list of vertices.
     */
    private List<NodeStorage> getNodeStorages(NodeStorage nodeStorage, GraphTraversalSource g, long snapshotId) throws OutDatedDataException
    {
        ArrayList<Vertex> nodeList =  getVertexList(nodeStorage, g, snapshotId);
        ArrayList<NodeStorage> returnStorage =  new ArrayList<>();

        for(Vertex vertex: nodeList)
        {
            NodeStorage storage = new NodeStorage(vertex.label());

            for(String key: vertex.keys())
            {
                storage.addProperty(key, vertex.property(key).value());
            }

            if(storage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
            {
                Object sId =  storage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                OutDatedDataException.checkSnapshotId(sId, snapshotId);
            }

            returnStorage.add(storage);
        }

        return returnStorage;
    }


    /**
     * Kills the graph database.
     */
    @Override
    public void terminate()
    {
        graph.close();;
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

    @Override
    public String getType()
    {
        return Constants.TITAN;
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
        if(graph == null)
        {
            start();
        }

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();
            GraphTraversal<Vertex, Vertex> tempOutput = g.V().hasLabel(nodeStorage.getId());

            for (Map.Entry<String, Object> entry : nodeStorage.getProperties().entrySet())
            {
                if (tempOutput == null)
                {
                    break;
                }
                tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
            }

            if(tempOutput != null && !HashCreator.sha1FromNode(nodeStorage).equals(tempOutput.values("hash").toString()))
            {
                return false;
            }
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Failed at generating hash in server " + id, e);
        }
        finally
        {
            graph.tx().commit();
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
        if(graph == null)
        {
            start();
        }

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();


            GraphTraversal<Vertex, Vertex> tempOutput = g.V().hasLabel(relationshipStorage.getId());

            for (Map.Entry<String, Object> entry : relationshipStorage.getProperties().entrySet())
            {
                if (tempOutput == null)
                {
                    break;
                }
                tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
            }

            if(tempOutput != null && !HashCreator.sha1FromRelationship(relationshipStorage).equals(tempOutput.values("hash").toString()))
            {
                return false;
            }
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Failed at generating hash in server " + id, e);
        }
        finally
        {
            graph.tx().commit();
        }

        return true;
    }

    //todo add hash on creation and update
    @Override
    public void execute(
            final List<NodeStorage> createSetNode,
            final List<RelationshipStorage> createSetRelationship,
            final Map<NodeStorage, NodeStorage> updateSetNode,
            final Map<RelationshipStorage, RelationshipStorage> updateSetRelationship,
            final List<NodeStorage> deleteSetNode,
            final List<RelationshipStorage> deleteSetRelationship, long snapshotId)
    {
        if(graph == null)
        {
            start();
        }

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            //Create node
            for (NodeStorage node : createSetNode)
            {
                TitanVertex vertex = graph.addVertex(node.getId());
                for (Map.Entry<String, Object> entry : node.getProperties().entrySet())
                {
                    vertex.property(entry.getKey(), entry.getValue());
                }
                vertex.property(Constants.TAG_HASH, HashCreator.sha1FromNode(node));
                vertex.property(Constants.TAG_SNAPSHOT_ID, snapshotId);

            }

            //Create relationships
            for (RelationshipStorage relationship : createSetRelationship)
            {
                GraphTraversal<Vertex,Vertex> startNode =  getVertexList(relationship.getStartNode(), g);
                GraphTraversal<Vertex,Vertex> endNode =  getVertexList(relationship.getEndNode(), g);

                final int length = relationship.getProperties().size() * 2 + 4;
                Object[] keyValue = new Object[length];

                int i = 0;
                for(Map.Entry<String, Object> entry: relationship.getProperties().entrySet())
                {
                    keyValue[i] = entry.getKey();
                    keyValue[i+1] = entry.getValue();
                    i += 2;
                }

                keyValue[i] = Constants.TAG_HASH;
                keyValue[i+1] = HashCreator.sha1FromRelationship(relationship);

                keyValue[i] = Constants.TAG_SNAPSHOT_ID;
                keyValue[i+1] = snapshotId;

                while(startNode.hasNext())
                {
                    Vertex tempVertex = startNode.next();
                    while (endNode.hasNext())
                    {
                        tempVertex.addEdge(relationship.getId(), endNode.next(), keyValue);
                    }
                }
            }

            //Can't change label in titan!
            //Update nodes
            for (Map.Entry<NodeStorage, NodeStorage> node : updateSetNode.entrySet())
            {
                GraphTraversal<Vertex,Vertex> tempNode =  getVertexList(node.getKey(), g);

                NodeStorage tempStorage = new NodeStorage(node.getValue().getId(), node.getKey().getProperties());
                for(Map.Entry<String, Object> entry: node.getValue().getProperties().entrySet())
                {
                    tempStorage.addProperty(entry.getKey(), entry.getValue());
                }


                while(tempNode.hasNext())
                {
                    Vertex vertex = tempNode.next();

                    final int length = node.getKey().getProperties().size() * 2 + 2;
                    Object[] keyValue = new Object[length];

                    int i = 0;
                    for(Map.Entry<String, Object> entry: node.getKey().getProperties().entrySet())
                    {
                        keyValue[i] = entry.getKey();
                        keyValue[i+1] = entry.getValue();
                        i += 2;
                    }

                    vertex.property(Constants.TAG_HASH, HashCreator.sha1FromNode(tempStorage), keyValue);
                    vertex.property(Constants.TAG_SNAPSHOT_ID, snapshotId);
                }
            }

            //Update relationships
            for (Map.Entry<RelationshipStorage, RelationshipStorage> relationship : updateSetRelationship.entrySet())
            {
                GraphTraversal<Vertex,Vertex> startNode =  getVertexList(relationship.getKey().getStartNode(), g);
                GraphTraversal<Vertex,Vertex> endNode =  getVertexList(relationship.getKey().getEndNode(), g);

                Set<String> keys = relationship.getKey().getProperties().keySet();
                keys.addAll(relationship.getValue().getProperties().keySet());

                //Max size is the mix between properties of both maps + 4 (hash and snapshotId)

                RelationshipStorage tempStorage = new RelationshipStorage(relationship.getValue().getId(), relationship.getKey().getProperties(), relationship.getKey().getStartNode(), relationship.getKey().getEndNode());
                for(Map.Entry<String, Object> entry: relationship.getValue().getProperties().entrySet())
                {
                    tempStorage.addProperty(entry.getKey(), entry.getValue());
                }

                while(startNode.hasNext())
                {
                    while(endNode.hasNext())
                    {
                        Iterator<Edge> edges = startNode.next().edges(Direction.OUT, relationship.getKey().getId());

                        while(edges.hasNext())
                        {
                            Edge edge = edges.next();
                            for(String key : keys)
                            {
                                //does a null value set the property to null?
                                Object value = relationship.getValue().getProperties().get(key);
                                edge.property(key, value);
                            }
                            edge.property(Constants.TAG_HASH, HashCreator.sha1FromRelationship(tempStorage));
                            edge.property(Constants.TAG_SNAPSHOT_ID, snapshotId);


                        }
                    }
                }
            }

            //Delete relationships
            for (RelationshipStorage relationship : deleteSetRelationship)
            {
                ArrayList<Vertex> nodeStartList =  getVertexList(relationship.getStartNode(), g, snapshotId);
                ArrayList<Vertex> nodeEndList =  getVertexList(relationship.getEndNode(), g, snapshotId);

                GraphTraversal<Vertex, Edge> tempOutput = g.V(nodeStartList.toArray()).bothE().where(__.is(P.within(nodeEndList.toArray()))).hasLabel(relationship.getId());


                for (Map.Entry<String, Object> entry : relationship.getProperties().entrySet())
                {
                    if (tempOutput == null)
                    {
                        break;
                    }
                    tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
                }

                if(tempOutput != null)
                {
                    if(tempOutput.has(Constants.TAG_SNAPSHOT_ID) == null || (tempOutput = tempOutput.has(Constants.TAG_SNAPSHOT_ID, P.lte(snapshotId))) != null)
                    {
                        tempOutput.remove();
                    }
                }
            }

            for (NodeStorage node : deleteSetNode)
            {
                GraphTraversal<Vertex,Vertex> tempNode =  getVertexList(node, g);

                while(tempNode.hasNext())
                {
                    tempNode.remove();
                }
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't create hash in server " + id, e);
        }
        finally
        {
            graph.tx().commit();
        }

    }

    /**
     * Gets the graph traversal object for a nodeStorage.
     * @param nodeStorage the storage.
     * @param g the graph.
     * @return the traversal object.
     */
    private GraphTraversal<Vertex, Vertex> getVertexList(final NodeStorage nodeStorage, final GraphTraversalSource g)
    {
        GraphTraversal<Vertex, Vertex> tempOutput = g.V().hasLabel(nodeStorage.getId());

        for (Map.Entry<String, Object> entry : nodeStorage.getProperties().entrySet())
        {
            if (tempOutput == null)
            {
                break;
            }
            tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
        }

        return tempOutput;
    }
}
