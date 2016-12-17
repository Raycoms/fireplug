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
 * Database access for the sparksee graph database.
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

    @Override
    public void start()
    {
        SparkseeProperties.load("config/sparksee.cfg");
        SparkseeConfig cfg = new SparkseeConfig();
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

    @Override
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
            sess.close();
            return Collections.emptyList();
        }

        ArrayList<Object> returnStorage = new ArrayList<>();

        if (nodeStorage == null)
        {
            NodeStorage startNode = relationshipStorage.getStartNode();
            NodeStorage endNode = relationshipStorage.getEndNode();

            Objects objsStart = findNode(graph, startNode);
            Objects objsEnd = findNode(graph, endNode);

            if (objsStart == null || objsStart.isEmpty() || objsEnd == null || objsEnd.isEmpty())
            {
                sess.close();
                return Collections.emptyList();
            }

            ObjectsIterator itStart = objsStart.iterator();
            ObjectsIterator itEnd = objsEnd.iterator();
            //todo if no type given, then what?
            //Sparkee, can't search for node or relationship without it's type set!
            int relationshipTypeId = graph.findType(relationshipStorage.getId());

            while (itStart.hasNext())
            {
                long localStartNodeId = itStart.next();
                while (itEnd.hasNext())
                {
                    long localEndNodeId = itEnd.next();

                    long edgeId = graph.findEdge(relationshipTypeId, localStartNodeId, localEndNodeId);
                    RelationshipStorage storage = getRelationshipFromRelationshipId(graph, edgeId);

                    if (storage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                    {
                        Object sId = storage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                        OutDatedDataException.checkSnapshotId(sId, localSnapshotId);
                        storage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                    }

                    returnStorage.add(storage);
                }
            }
            objsStart.close();
            objsEnd.close();
            itStart.close();
            itEnd.close();
        }
        else
        {
            Objects objs = findNode(graph, nodeStorage);

            if (objs == null || objs.isEmpty())
            {
                sess.close();
                return Collections.emptyList();
            }

            for (final Long nodeId : objs)
            {
                NodeStorage tempStorage = getNodeFromNodeId(graph, nodeId);

                if (tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                {
                    Object sId = tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                    OutDatedDataException.checkSnapshotId(sId, localSnapshotId);
                    tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                }
                returnStorage.add(tempStorage);
            }

            objs.close();
        }

        sess.close();
        return returnStorage;
    }

    /**
     * Creates a RelationshipStorage from the edgeId.
     * @param graph the graph.
     * @param edgeId the edgeId.
     * @return the relationshipStorage.
     */
    private RelationshipStorage getRelationshipFromRelationshipId(Graph graph, long edgeId)
    {
        Iterator<Integer> attributes = graph.getAttributes(edgeId).iterator();
        Map<String, Object> localProperties = new HashMap<>();
        while (attributes.hasNext())
        {
            String attributeKey = graph.getAttributeText(edgeId, attributes.next()).toString();
            Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(edgeId, attributes.next()));
            localProperties.put(attributeKey, attributeValue);
        }

        NodeStorage tempStartNode = getNodeFromNodeId(graph, graph.getEdgeData(edgeId).getTail());
        NodeStorage tempEndNode = getNodeFromNodeId(graph, graph.getEdgeData(edgeId).getTail());

        return new RelationshipStorage(graph.getType(graph.getObjectType(edgeId)).getName(), localProperties, tempStartNode, tempEndNode);
    }

    /**
     * Creates a NodeStorage from the nodeId.
     * @param graph the graph.
     * @param nodeId the nodeId.
     * @return the nodeStorage.
     */
    private NodeStorage getNodeFromNodeId(Graph graph, long nodeId)
    {
        AttributeListIterator attributes = graph.getAttributes(nodeId).iterator();
        Map<String, Object> localProperties = new HashMap<>();
        while (attributes.hasNext())
        {
            String attributeKey = graph.getAttributeText(nodeId, attributes.next()).toString();
            Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(nodeId, attributes.next()));
            localProperties.put(attributeKey, attributeValue);
        }

        return new NodeStorage(graph.getType(graph.getObjectType(nodeId)).getName(), localProperties);
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        NodeStorage startNode = storage.getStartNode();
        NodeStorage endNode = storage.getEndNode();

        Objects objsStart = findNode(graph, startNode);
        Objects objsEnd = findNode(graph, endNode);

        if (objsStart == null || objsStart.isEmpty() || objsEnd == null || objsEnd.isEmpty())
        {
            if(objsStart != null)
            {
                objsStart.close();
            }
            if(objsEnd != null)
            {
                objsEnd.close();
            }
            sess.close();
            return false;
        }

        ObjectsIterator itStart = objsStart.iterator();
        ObjectsIterator itEnd = objsEnd.iterator();

        int relationshipTypeId = graph.findType(storage.getId());

        try
        {
            long oId = graph.findEdge(relationshipTypeId, itStart.next(), itEnd.next());
            return HashCreator.sha1FromRelationship(storage).equals(graph.getAttribute(oId, graph.findAttribute(Type.getGlobalType(), "hash")).getString());
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
        return false;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        Objects objs = findNode(graph, storage);

        if (objs == null || objs.isEmpty())
        {
            if(objs != null)
            {
                objs.close();
            }
            sess.close();
            return false;
        }

        ObjectsIterator it = objs.iterator();

        try
        {
            long oId = it.next();
            return HashCreator.sha1FromNode(storage).equals(graph.getAttribute(oId, graph.findAttribute(Type.getGlobalType(), "hash")).getString());
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
        return false;
    }

    /**
     * Return a Objects array matching the nodeType and properties.
     * @param graph the graph.
     * @param storage the storage of the node.
     * @return Objects which match the attributes.
     */
    private Objects findNode(Graph graph, NodeStorage storage)
    {
        Objects objs = null;

        if(!storage.getId().isEmpty())
        {
            int nodeTypeId = SparkseeUtils.createOrFindNodeType(storage, graph);
            objs = graph.select(nodeTypeId);
        }

        for (Map.Entry<String, Object> entry : storage.getProperties().entrySet())
        {
            int attributeId = graph.findAttribute(Type.getGlobalType(), entry.getKey());

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
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        Objects objs = findNode(graph, key);
        if (objs == null || objs.isEmpty())
        {
            if(objs != null)
            {
                objs.close();
            }
            return false;
        }

        ObjectsIterator it = objs.iterator();

        while(it.hasNext())
        {
            long nodeId = it.next();

            for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
            {
                int attributeTypeId = SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.getGlobalType(), graph);
                graph.setAttribute(nodeId, attributeTypeId, SparkseeUtils.getValue(entry.getValue()));
            }

            int attributeTypeIdHash = SparkseeUtils.createOrFindAttributeType(Constants.TAG_HASH, "", Type.getGlobalType(), graph);

            try
            {
                graph.setAttribute(nodeId, attributeTypeIdHash, SparkseeUtils.getValue(HashCreator.sha1FromNode(getNodeFromNodeId(graph, nodeId))));
            }
            catch (NoSuchAlgorithmException e)
            {
                Log.getLogger().warn("Couldn't execute update node transaction in server:  " + id, e);
                return false;
            }
            finally
            {
                objs.close();
                it.close();
                sess.close();
            }

            int attributeTypeIdSnapshotId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, 1L, Type.getGlobalType(), graph);
            graph.setAttribute(nodeId, attributeTypeIdSnapshotId, SparkseeUtils.getValue(snapshotId));
        }

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
            int attributeId = SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.GlobalType, graph);
            graph.setAttribute(nodeId, attributeId, SparkseeUtils.getValue(entry.getValue()));
        }

        int snapshotAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, snapshotId, Type.GlobalType, graph);
        graph.setAttribute(nodeId, snapshotAttributeId, SparkseeUtils.getValue(snapshotId));

        try
        {
            int hashAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_HASH, " ", Type.GlobalType, graph);
            graph.setAttribute(nodeId, hashAttributeId, SparkseeUtils.getValue(HashCreator.sha1FromNode(storage)));
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
            return false;
        }
        finally
        {
            sess.close();
        }

        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        Objects objs = findNode(graph, storage);

        if(objs != null)
        {
            graph.drop(objs);
            objs.close();
        }

        sess.close();
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        Objects startObjs = findNode(graph, key.getStartNode());
        Objects endObjs = findNode(graph, key.getStartNode());

        if(startObjs == null || endObjs == null)
        {
            if(startObjs != null)
            {
                startObjs.close();
            }
            if(endObjs != null)
            {
                endObjs.close();
            }
            sess.close();
            return false;
        }

        ObjectsIterator startIt = startObjs.iterator();
        ObjectsIterator endIt = endObjs.iterator();

        while(startIt.hasNext())
        {
            long startNode = startIt.next();
            while(endIt.hasNext())
            {
                long endNode = endIt.next();
                long relationship = graph.findEdge(graph.findType(key.getId()), startNode, endNode);

                for (Map.Entry<String, Object> entry : value.getProperties().entrySet())
                {
                    int attributeTypeId = SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.getGlobalType(), graph);
                    graph.setAttribute(relationship, attributeTypeId, SparkseeUtils.getValue(entry.getValue()));
                }

                int attributeTypeIdHash = SparkseeUtils.createOrFindAttributeType(Constants.TAG_HASH, "", Type.getGlobalType(), graph);

                try
                {
                    graph.setAttribute(relationship, attributeTypeIdHash, SparkseeUtils.getValue(HashCreator.sha1FromRelationship(getRelationshipFromRelationshipId(graph, relationship))));
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.getLogger().warn("Couldn't execute update node transaction in server:  " + id, e);
                    return false;
                }
                finally
                {
                    startObjs.close();
                    endObjs.close();
                    startIt.close();
                    endIt.close();
                    sess.close();
                }

                int attributeTypeIdSnapshotId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, 1L, Type.getGlobalType(), graph);
                graph.setAttribute(relationship, attributeTypeIdSnapshotId, SparkseeUtils.getValue(snapshotId));
            }
        }

        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        Objects startObjs = findNode(graph, storage.getStartNode());
        Objects endObjs = findNode(graph, storage.getStartNode());

        if(startObjs == null || endObjs == null)
        {
            if(startObjs != null)
            {
                startObjs.close();
            }
            if(endObjs != null)
            {
                endObjs.close();
            }
            sess.close();
            return false;
        }

        ObjectsIterator startIt = startObjs.iterator();
        ObjectsIterator endIt = endObjs.iterator();

        while(startIt.hasNext())
        {
            long startNode = startIt.next();
            while(endIt.hasNext())
            {
                long endNode = endIt.next();

                long relationship = graph.findOrCreateEdge(graph.findType(storage.getId()), startNode, endNode);
                for(Map.Entry<String, Object> entry : storage.getProperties().entrySet())
                {
                    graph.setAttribute(relationship, SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.GlobalType, graph), SparkseeUtils.getValue(entry.getValue()));

                    int snapshotAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, snapshotId, Type.GlobalType, graph);
                    graph.setAttribute(relationship, snapshotAttributeId, SparkseeUtils.getValue(snapshotId));

                    try
                    {
                        int hashAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_HASH, " ", Type.GlobalType, graph);
                        graph.setAttribute(relationship, hashAttributeId, SparkseeUtils.getValue(HashCreator.sha1FromRelationship(storage)));
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.getLogger().warn("Couldn't execute create node transaction in server:  " + id, e);
                        return false;
                    }
                    finally
                    {
                        sess.close();
                        endObjs.close();
                        startObjs.close();
                        startIt.close();
                        endIt.close();
                    }
                }
            }
        }

        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        Objects startObjs = findNode(graph, storage.getStartNode());
        Objects endObjs = findNode(graph, storage.getStartNode());

        if(startObjs == null || endObjs == null)
        {
            if(startObjs != null)
            {
                startObjs.close();
            }
            if(endObjs != null)
            {
                endObjs.close();
            }
            sess.close();
            return false;
        }

        ObjectsIterator startIt = startObjs.iterator();
        ObjectsIterator endIt = endObjs.iterator();

        while(startIt.hasNext())
        {
            long startNode = startIt.next();
            while(endIt.hasNext())
            {
                long endNode = endIt.next();

                graph.drop(graph.findEdge(graph.findType(storage.getId()), startNode, endNode));
            }
        }

        startIt.close();
        endIt.close();
        startObjs.close();
        endObjs.close();
        sess.close();
        return true;
    }
}
