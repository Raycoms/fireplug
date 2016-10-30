package main.java.com.bag.server.database;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import main.java.com.bag.server.database.Interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
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
    public static final String INDEX_NAME = "search";

    public static final String directory ="";

    private TitanGraph graph;

    private int id;

    public void start(int id)
    {
        this.id = id;
        TitanFactory.Builder config = TitanFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", directory);
        config.set("index." + INDEX_NAME + ".backend", "elasticsearch");
        config.set("index." + INDEX_NAME + ".directory", directory + File.separator + "es");
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
    public List<Object> readObject(@NotNull Object identifier)
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
        ArrayList<Vertex> nodeList =  new ArrayList<>();
        ArrayList<Edge> relationshipList =  new ArrayList<>();

        try
        {
            graph.newTransaction();
            GraphTraversalSource g = graph.traversal();
            //todo also return hash and snapshot id property (later on when we added it, for node and relationship)

            //If nodeStorage is null, we're obviously trying to read relationships.
            if(nodeStorage == null)
            {
                GraphTraversal<Edge, Edge> tempOutput = g.E().hasLabel(relationshipStorage.getId());

                if(relationshipStorage.getProperties() != null)
                {
                    for (Map.Entry<String, Object> entry : relationshipStorage.getProperties().entrySet())
                    {
                        if (tempOutput == null)
                        {
                            break;
                        }
                        tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
                    }
                }
                
                if(tempOutput != null)
                {
                    tempOutput.fill(relationshipList);
                }
            }
            else
            {
                returnStorage.addAll(getNodeStorages(nodeStorage, g));
            }
        }
        finally
        {
            graph.tx().commit();
        }


        return returnStorage;
    }

    private List<NodeStorage> getNodeStorages(NodeStorage nodeStorage, GraphTraversalSource g)
    {
        GraphTraversal<Vertex, Vertex> tempOutput = g.V().hasLabel(nodeStorage.getId());
        ArrayList<Vertex> nodeList =  new ArrayList<>();
        ArrayList<NodeStorage> returnStorage =  new ArrayList<>();

        if(nodeStorage.getProperties() != null)
        {
            for (Map.Entry<String, Object> entry : nodeStorage.getProperties().entrySet())
            {
                if (tempOutput == null)
                {
                    break;
                }
                tempOutput = tempOutput.has(entry.getKey(), entry.getValue());
            }
        }

        if(tempOutput != null)
        {
            tempOutput.fill(nodeList);
        }

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

    public void terminate()
    {
        graph.close();
    }
}
