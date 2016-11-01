package main.java.com.bag.server.database;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class created to handle access to the titan database.
 */
public class TitanDatabaseAccess implements IDatabaseAccess
{
    private static final String INDEX_NAME = "search";

    private static final String DIRECTORY ="";

    private TitanGraph graph;

    private int id;

    public void start(int id)
    {
        this.id = id;
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

        if(graph == null)
        {
            start(id);
        }

        ArrayList<Object> returnStorage =  new ArrayList<>();

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();
            //todo also return hash and snapshot id property (later on when we added it, for node and relationship)

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
        GraphTraversal<Vertex, Vertex> tempOutput = g.V().hasLabel(nodeStorage.getId());
        ArrayList<Vertex> nodeList =  new ArrayList<>();

        for (Map.Entry<String, Object> entry : nodeStorage.getProperties().entrySet())
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
                tempOutput.fill(nodeList);
            }
        }

        return nodeList;
    }

    /**
     * Creates a list of vertices matching a certain nodeStorage.
     * @param nodeStorage the key.
     * @param g the graph.
     * @return the list of vertices.
     */
    private List<NodeStorage> getNodeStorages(NodeStorage nodeStorage, GraphTraversalSource g, long snapshotId)
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
            returnStorage.add(storage);
        }

        return returnStorage;
    }

    /**
     * Terminates the graph database.
     */
    @Override
    public void terminate()
    {
        graph.close();
    }
}
