package main.java.com.bag.server.database;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jetbrains.annotations.NotNull;

import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class created to handle access to the titan database.
 */
public class TitanDatabaseAccess implements IDatabaseAccess
{
    private static final String DIRECTORY = System.getProperty("user.home") + "/TitanDB";

    private TitanGraph graph;

    private final int id;

    public TitanDatabaseAccess(int id)
    {
        this.id = id;
    }

    @Override
    public void start()
    {
        //Logger.getLogger(com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx.class).setLevel(Level.OFF);
        TitanFactory.Builder config = TitanFactory.build();

        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", DIRECTORY);
        graph = config.open();
        TitanManagement mg = graph.openManagement();
        PropertyKey idxKey = mg.getPropertyKey("idx");
        if (idxKey == null) {
            idxKey = mg.makePropertyKey("idx").dataType(String.class).make();
            mg.buildIndex("byIdx", Vertex.class).addKey(idxKey).buildCompositeIndex();
        }
        mg.commit();
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
            Log.getLogger().info("Can't read data on object: " + identifier.getClass().toString());
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
                returnStorage.addAll(getRelationshipStorages(relationshipStorage, g, snapshotId));
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

        GraphTraversal<Vertex, Edge> tempOutput =  graph.traversal().V(nodeStartList.toArray()).bothE().filter(__.otherV().is(P.within(nodeEndList.toArray())));

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
            tempOutput.fill(relationshipList);
        }

        ArrayList<RelationshipStorage> returnList = new ArrayList<>();

        for(Edge edge: relationshipList)
        {
            RelationshipStorage tempStorage = getRelationshipStorageFromEdge(edge);
            if(tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
            {
                Object sId =  tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                OutDatedDataException.checkSnapshotId(sId, snapshotId);
                tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
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

        if(tempOutput!= null)
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
            NodeStorage tempStorage = getNodeStorageFromVertex(vertex);

            if(tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
            {
                Object sId =  tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                OutDatedDataException.checkSnapshotId(sId, snapshotId);
                tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
            }

            returnStorage.add(tempStorage);
        }

        return returnStorage;
    }

    /**
     * Generated a NodeStorage from a Vertex.
     * @param tempVertex the base vertex.
     * @return the nodeStorage.
     */
    private NodeStorage getNodeStorageFromVertex(Vertex tempVertex)
    {
        NodeStorage tempStorage = new NodeStorage(tempVertex.label());

        for(String key: tempVertex.keys())
        {
            if(key.equals(Constants.TAG_SNAPSHOT_ID))
            {
                continue;
            }
            tempStorage.addProperty(key, tempVertex.property(key).value());
        }
        return tempStorage;
    }

    /**
     * Generated a RelationshipStorage from an Edge.
     * @param edge the base edge.
     * @return the relationshipStorage.
     */
    private RelationshipStorage getRelationshipStorageFromEdge(Edge edge)
    {
        RelationshipStorage tempStorage = new RelationshipStorage(edge.label(), getNodeStorageFromVertex(edge.outVertex()), getNodeStorageFromVertex(edge.inVertex()));
        for(String s: edge.keys())
        {
            tempStorage.addProperty(s, edge.property(s));
        }
        return tempStorage;
    }

    /**
     * Kills the graph database.
     */
    @Override
    public void terminate()
    {
        graph.close();
    }

    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param nodeStorage the node to compare
     * @return true if equal hash, else false.
     */
    @Override
    public boolean compareNode(final NodeStorage nodeStorage)
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

            if(tempOutput == null || !HashCreator.sha1FromNode(nodeStorage).equals(tempOutput.values("hash").toString()))
            {
                return false;
            }
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.getLogger().info("Failed at generating hash in server " + id, e);
        }
        finally
        {
            graph.tx().commit();
        }

        return true;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            //Can't change label in titan!

            GraphTraversal<Vertex, Vertex> tempNode = getVertexList(key, g);

            while (tempNode.hasNext())
            {
                Vertex vertex = tempNode.next();

                for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
                {
                    vertex.property(entry.getKey(), entry.getValue());
                }
                vertex.property(Constants.TAG_HASH, HashCreator.sha1FromNode(getNodeStorageFromVertex(vertex)));
                vertex.property(Constants.TAG_SNAPSHOT_ID, snapshotId);
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute update node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed update node transaction in server:  " + id);

        return true;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        try
        {
            graph.newTransaction();

            TitanVertex vertex = graph.addVertex(storage.getId());
            for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
            {
                vertex.property(entry.getKey(), entry.getValue());
            }
            vertex.property(Constants.TAG_HASH, HashCreator.sha1FromNode(storage));
            vertex.property(Constants.TAG_SNAPSHOT_ID, snapshotId);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed create node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            GraphTraversal<Vertex, Vertex> tempNode = getVertexList(storage, g);

            while (tempNode.hasNext())
            {
                tempNode.next().remove();
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed delete node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            GraphTraversal<Vertex, Vertex> startNode = getVertexList(key.getStartNode(), g);
            GraphTraversal<Vertex, Vertex> endNode = getVertexList(key.getEndNode(), g);

            //Max size is the mix between properties of both maps + 4 (hash and snapshotId)

            RelationshipStorage tempStorage = new RelationshipStorage(value.getId(),
                    key.getProperties(),
                    key.getStartNode(),
                    key.getEndNode());
            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                tempStorage.addProperty(entry.getKey(), entry.getValue());
            }

            while (startNode.hasNext())
            {
                while (endNode.hasNext())
                {
                    Iterator<Edge> edges = startNode.next().edges(Direction.OUT, key.getId());

                    while (edges.hasNext())
                    {
                        Edge edge = edges.next();
                        for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
                        {
                            edge.property(entry.getKey(), entry.getValue());
                        }
                        edge.property(Constants.TAG_HASH, HashCreator.sha1FromRelationship(getRelationshipStorageFromEdge(edge)));
                        edge.property(Constants.TAG_SNAPSHOT_ID, snapshotId);
                    }
                }
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute update relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed update relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            GraphTraversal<Vertex, Vertex> startNode = getVertexList(storage.getStartNode(), g);
            GraphTraversal<Vertex, Vertex> endNode = getVertexList(storage.getEndNode(), g);

            while (startNode.hasNext())
            {
                Vertex tempVertex = startNode.next();
                while (endNode.hasNext())
                {
                    Edge edge  = tempVertex.addEdge(storage.getId(), endNode.next());

                    edge.property(Constants.TAG_HASH, HashCreator.sha1FromRelationship(storage));
                    edge.property(Constants.TAG_SNAPSHOT_ID, snapshotId);

                    for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
                    {
                        edge.property(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute create relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed create relationship transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();

            ArrayList<Vertex> nodeStartList = getVertexList(storage.getStartNode(), g, snapshotId);
            ArrayList<Vertex> nodeEndList = getVertexList(storage.getEndNode(), g, snapshotId);
            GraphTraversal<Vertex, Edge> tempOutput = g.V(nodeStartList.toArray()).bothE().where(__.is(P.within(nodeEndList.toArray()))).hasLabel(storage.getId());

            for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
            {
                if (tempOutput == null)
                {
                    break;
                }
                tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
            }

            if (tempOutput != null && (tempOutput.has(Constants.TAG_SNAPSHOT_ID) == null || (tempOutput = tempOutput.has(Constants.TAG_SNAPSHOT_ID, P.lte(snapshotId))) != null))
            {

                tempOutput.remove();
            }
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't execute delete relationship transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            graph.tx().commit();
        }
        Log.getLogger().info("Successfully executed delete relationship transaction in server:  " + id);
        return true;
    }

    /**
     * Compares a nodeStorage with the node inside the db to check if correct.
     * @param relationshipStorage the node to compare
     * @return true if equal hash, else false.
     */
    @Override
    public boolean compareRelationship(final RelationshipStorage relationshipStorage)
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

            if(tempOutput == null || !HashCreator.sha1FromRelationship(relationshipStorage).equals(tempOutput.values("hash").toString()))
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
