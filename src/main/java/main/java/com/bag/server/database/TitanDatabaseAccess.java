package main.java.com.bag.server.database;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import main.java.com.bag.server.database.Interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

         //graph = TinkerGraph.open();
        //todo get info from the list and parse all nodes which match this.
        ArrayList<Object> returnStorage =  new ArrayList<>();
        GremlinPipeline pipe = new GremlinPipeline();

        //todo stack .has().has().has() to get various properties.
        //todo add gremlin access to get it through gremlin, probably need a similar approach in orientDB and neo4j as well.
        try
        {
            GraphTraversalSource g = graph.traversal();
            Vertex fromNode = g.V().has("name", "marko").next();
            Vertex toNode = g.V().has("name", "peter").next();
            ArrayList list = new ArrayList();


            graph.newTransaction();

            //If nodeStorage is null, we're obviously trying to read relationships.
            if(nodeStorage == null)
            {
                relationshipStorage.getId();
            }
            else
            {
                nodeStorage.getId();
            }

            //graph.V().has('name', 'hercules')
            //odb.createVertexType("Person"); this is our class (neo4j label)

        }
        finally
        {
            graph.tx().commit();
        }

        return returnStorage;
    }

    public void terminate()
    {
        graph.close();
    }
}
