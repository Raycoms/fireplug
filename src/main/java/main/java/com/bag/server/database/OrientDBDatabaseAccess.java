package main.java.com.bag.server.database;

import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import main.java.com.bag.server.database.Interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class created to handle access to the OrientDB database.
 */
public class OrientDBDatabaseAccess implements IDatabaseAccess
{
    private static final String BASE_PATH = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/OrientDB";


    private int id;
    private OrientGraphFactory factory;

    public void start(int id)
    {
        this.id = id;
        factory = new OrientGraphFactory("BASE_PATH").setupPool(1,10);
    }


    /*
    OrientGraph graph = factory.getTx();
try {
  ...

} finally {
   graph.shutdown();
}
     */

    /*try{
  Vertex luca = graph.addVertex(null); // 1st OPERATION: IMPLICITLY BEGIN A TRANSACTION
  luca.setProperty( "name", "Luca" );
  Vertex marko = graph.addVertex(null);
  marko.setProperty( "name", "Marko" );
  Edge lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows");
  graph.commit();
} catch( Exception e ) {
  graph.rollback();
}*/


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

        if(factory == null)
        {
            start(id);
        }

        //todo get info from the list and parse all nodes which match this.
        ArrayList<Object> returnStorage =  new ArrayList<>();

        OrientGraph graph = factory.getTx();
        try
        {
            //If nodeStorage is null, we're obviously trying to read relationships.
            if(nodeStorage == null)
            {
                relationshipStorage.getId();
            }
            else
            {
                nodeStorage.getId();
            }

            //odb.createVertexType("Person"); this is our class (neo4j label)

        }
        finally
        {
            graph.shutdown();
        }

        return returnStorage;
    }


    public void terminate()
    {

    }
}
