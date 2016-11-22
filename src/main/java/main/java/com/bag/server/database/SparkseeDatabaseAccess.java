package main.java.com.bag.server.database;

import com.sparsity.sparksee.gdb.*;
import com.sparsity.sparksee.gdb.Objects;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.HashCreator;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.FileNotFoundException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Database access for the arangoDB database.
 */
public class SparkseeDatabaseAccess implements IDatabaseAccess
{
    private final int id;
    private Database db = null;
    private Sparksee sparksee;
    public SparkseeDatabaseAccess(int id)
    {
        this.id = id;
    }

    public void start()
    {
        SparkseeConfig cfg = new SparkseeConfig();
        cfg.setCacheMaxSize(1024);
        sparksee = new Sparksee(cfg);
        try
        {
            db = sparksee.create("HelloSparksee.gdb", "HelloSparksee");
        }
        catch (FileNotFoundException e)
        {
            Log.getLogger().warn("Couldn't start sparksee", e);
        }
    }

    public void terminate()
    {
        db.close();
        sparksee.close();
    }


    @Override
    public List<Object> readObject(final Object identifier, final long localSnapshotId) throws OutDatedDataException
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

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

        ArrayList<Object> returnStorage = new ArrayList<>();

        if (nodeStorage == null)
        {
            NodeStorage startNode = relationshipStorage.getStartNode();
            NodeStorage endNode = relationshipStorage.getEndNode();
            int nodeTypeStart = graph.findType(startNode.getId());
            int nodeTypeEnd = graph.findType(endNode.getId());

            Objects objsStart = findNode(graph, startNode, nodeTypeStart);
            Objects objsEnd = findNode(graph, startNode, nodeTypeEnd);

            if (objsStart == null || objsStart.isEmpty() || objsEnd == null || objsEnd.isEmpty())
            {
                return Collections.emptyList();
            }

            ObjectsIterator itStart = objsStart.iterator();
            ObjectsIterator itEnd = objsEnd.iterator();

            //Sparkee, can't search for node or relationship without it's type set!
            int relationshipTypeId = graph.findType(relationshipStorage.getId());

            while (itStart.hasNext())
            {
                long localStartNodeId = itStart.next();
                while (itEnd.hasNext())
                {
                    long localEndNodeId = itEnd.next();
                    long edgeId = graph.findEdge(relationshipTypeId, localStartNodeId, localEndNodeId);
                    Iterator<Integer> attributes = graph.getAttributes(edgeId).iterator();
                    Map<String, Object> localProperties = new HashMap<>();
                    while (attributes.hasNext())
                    {
                        String attributeKey = graph.getAttributeText(edgeId, attributes.next()).toString();
                        Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(edgeId, attributes.next()));
                        localProperties.put(attributeKey, attributeValue);
                    }

                    boolean relationshipMatches = true;

                    for (Map.Entry<String, Object> entry : relationshipStorage.getProperties().entrySet())
                    {
                        if (localProperties.containsKey(entry.getKey()) && !localProperties.get(entry.getKey()).equals(entry.getValue()))
                        {
                            relationshipMatches = false;
                        }
                    }

                    if (relationshipMatches)
                    {
                        //todo might update the nodeStorages as well
                        RelationshipStorage storage = new RelationshipStorage(relationshipStorage.getId(), localProperties, startNode, endNode);
                        returnStorage.add(storage);
                    }
                }
            }
        }
        else
        {
            int nodeType = graph.findType(nodeStorage.getId());
            Objects objs = findNode(graph, nodeStorage, nodeType);

            if (objs == null || objs.isEmpty())
            {
                return Collections.emptyList();
            }

            for (final Long nodeId : objs)
            {
                Iterator<Integer> attributes = graph.getAttributes(nodeId).iterator();
                Map<String, Object> localProperties = new HashMap<>();
                while (attributes.hasNext())
                {
                    String attributeKey = graph.getAttributeText(nodeId, attributes.next()).toString();
                    Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(nodeId, attributes.next()));
                    localProperties.put(attributeKey, attributeValue);
                }

                boolean nodeMatches = true;
                for (Map.Entry<String, Object> entry : nodeStorage.getProperties().entrySet())
                {
                    if (localProperties.containsKey(entry.getKey()) && !localProperties.get(entry.getKey()).equals(entry.getValue()))
                    {
                        nodeMatches = false;
                    }
                }

                if (nodeMatches)
                {
                    returnStorage.add(new NodeStorage(nodeStorage.getId(), localProperties));
                }
            }
        }

        sess.close();

        return returnStorage;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        NodeStorage startNode = storage.getStartNode();
        NodeStorage endNode = storage.getEndNode();
        int nodeTypeStart = graph.findType(startNode.getId());
        int nodeTypeEnd = graph.findType(endNode.getId());

        Objects objsStart = findNode(graph, startNode, nodeTypeStart);
        Objects objsEnd = findNode(graph, startNode, nodeTypeEnd);

        if (objsStart == null || objsStart.isEmpty() || objsEnd == null || objsEnd.isEmpty())
        {
            return false;
        }

        ObjectsIterator itStart = objsStart.iterator();
        ObjectsIterator itEnd = objsEnd.iterator();

        int relationshipTypeId = graph.findType(storage.getId());

        try
        {
            long oId = graph.findEdge(relationshipTypeId, itStart.next(), itEnd.next());
            return HashCreator.sha1FromRelationship(storage).equals(graph.getAttribute(oId, graph.findAttribute(relationshipTypeId, "hash")).getString());
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute SHA1 for node", e);
        }
        finally
        {
            sess.close();
            itStart.close();
            itEnd.close();
            objsStart.close();
            objsEnd.close();
        }

        sess.close();
        return false;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        int nodeType = graph.findType(storage.getId());
        Objects objs = findNode(graph, storage, nodeType);

        if (objs == null || objs.isEmpty())
        {
            return false;
        }

        ObjectsIterator it = objs.iterator();

        try
        {
            long oId = it.next();
            return HashCreator.sha1FromNode(storage).equals(graph.getAttribute(oId, graph.findAttribute(nodeType, "hash")).getString());
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute SHA1 for node", e);
        }
        finally
        {
            sess.close();
            it.close();
            objs.close();
        }

        sess.close();
        return false;
    }

    /**
     * Return a Objects array matching the nodeType and properties.
     * @param graph the graph.
     * @param storage the storage of the node.
     * @param nodeType the node type.
     * @return Objects which match the attributes.
     */
    private Objects findNode(Graph graph, NodeStorage storage, int nodeType)
    {
        Objects objs = null;
        for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
        {
            int attributeId = graph.findAttribute(nodeType, entry.getKey());

            if (objs == null)
            {
                objs = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(entry.getValue()));
            }
            else
            {
                objs = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(entry.getValue()), objs);
            }

            if (objs.isEmpty())
            {
                return null;
            }
        }
        return objs;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        return false;
    }




    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        int nodeTypeId = SparkseeUtils.createOrFindNodeType(storage, graph);

        long nodeId = graph.newNode(nodeTypeId);

        for(Map.Entry<String, Object> entry : storage.getProperties().entrySet())
        {
            int attributeId = SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), nodeTypeId, graph);
            graph.setAttribute(nodeId, attributeId, SparkseeUtils.getValue(entry.getValue()));
        }

        int snapshotAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, snapshotId, nodeTypeId, graph);
        graph.setAttribute(nodeId, snapshotAttributeId, SparkseeUtils.getValue(snapshotId));

        try
        {
            int hashAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_HASH, " ", nodeTypeId, graph);
            graph.setAttribute(nodeId, hashAttributeId, SparkseeUtils.getValue(HashCreator.sha1FromNode(storage)));
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }

        sess.close();
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        return false;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        return false;
    }
}
