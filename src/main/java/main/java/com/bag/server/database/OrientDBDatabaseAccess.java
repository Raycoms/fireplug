package main.java.com.bag.server.database;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
            start(id);
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

    private Iterable<Vertex> getVertexList(final NodeStorage nodeStorage, final OrientGraph graph)
    {
        String[] propertyKeys   = nodeStorage.getProperties().keySet().toArray(new String[0]);
        Object[] propertyValues = nodeStorage.getProperties().values().toArray();
        return graph.getVertices(nodeStorage.getId(), propertyKeys , propertyValues);
    }

    public void terminate()
    {

    }
}
