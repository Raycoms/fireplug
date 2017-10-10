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

import java.io.File;
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
        Log.getLogger().warn("Starting Sparksee database service on " + id);
        SparkseeProperties.load("config/sparksee.cfg");
        final SparkseeConfig cfg = new SparkseeConfig();
        sparksee = new Sparksee(cfg);


        final String location = System.getProperty("user.home") + File.separator + "Sparksee" + this.id +
                File.separator + "HelloSparksee.gdb";

        try
        {
            db = sparksee.open(location, false);
        } catch (Exception e1)
        {
            Log.getLogger().warn("Unable to open Sparksee.");
            try
            {
                db = sparksee.create(location, "HelloSparksee");
            } catch (Exception e2)
            {
                Log.getLogger().error("Unable to create an instance of Sparksee!");
            }
        }
    }

    @Override
    public void terminate()
    {
        db.close();
        sparksee.close();
    }

    public List<Long> loadNodesAsIdList(Graph graph, NodeStorage node)
    {
        List<Long> ids = new ArrayList<>();
        Objects objects = findNode(graph, node);
        if (objects != null)
        {
            for (Long nodeId : objects)
            {
                ids.add(nodeId);
            }

            objects.close();
        }
        return ids;
    }

    @Override
    public List<Object> readObject(final Object identifier, final long localSnapshotId) throws OutDatedDataException
    {
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

        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        try
        {
            if (nodeStorage == null)
            {
                final NodeStorage startNode = relationshipStorage.getStartNode();
                final NodeStorage endNode = relationshipStorage.getEndNode();
                final List<Long> objsEnd = loadNodesAsIdList(graph, endNode);

                if (relationshipStorage.getStartNode().getProperties().isEmpty())
                {
                    //Sparkee can't search for node or relationship without it's type set!
                    int relationshipTypeId = graph.findType(relationshipStorage.getId());

                    for (long localEndNodeId : objsEnd)
                    {
                        final Objects connected = graph.explode(localEndNodeId, relationshipTypeId, EdgesDirection.Ingoing);

                        if (connected.isEmpty())
                        {
                            connected.close();
                            continue;
                        }

                        final ObjectsIterator iterator = connected.iterator();
                        try
                        {
                            while (iterator.hasNext())
                            {
                                final RelationshipStorage storage = getRelationshipFromRelationshipId(graph, iterator.next());

                                if (storage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                                {
                                    final Object sId = storage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                                    OutDatedDataException.checkSnapshotId(sId, localSnapshotId);
                                    storage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                                }
                                storage.removeProperty(Constants.TAG_HASH);
                                returnStorage.add(storage);
                            }
                        }
                        finally
                        {
                            iterator.close();
                            connected.close();
                        }
                    }
                }
                else
                {
                    final List<Long> objsStart = loadNodesAsIdList(graph, startNode);

                    //Sparkee, can't search for node or relationship without it's type set!
                    int relationshipTypeId = graph.findType(relationshipStorage.getId());

                    for (long localStartNodeId : objsStart)
                    {
                        for (long localEndNodeId : objsEnd)
                        {
                            final long edgeId = graph.findEdge(relationshipTypeId, localStartNodeId, localEndNodeId);
                            if (edgeId == 0)
                                continue;

                            final RelationshipStorage storage = getRelationshipFromRelationshipId(graph, edgeId);

                            if (storage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                            {
                                final Object sId = storage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                                OutDatedDataException.checkSnapshotId(sId, localSnapshotId);
                                storage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                            }
                            storage.removeProperty(Constants.TAG_HASH);

                            returnStorage.add(storage);
                        }
                    }
                }
            }
            else
            {
                Objects objs = findNode(graph, nodeStorage);

                if (objs == null)
                {
                    return Collections.emptyList();
                }
                try
                {
                    for (final Long nodeId : objs)
                    {
                        NodeStorage tempStorage = getNodeFromNodeId(graph, nodeId);

                        if (tempStorage.getProperties().containsKey(Constants.TAG_SNAPSHOT_ID))
                        {
                            Object sId = tempStorage.getProperties().get(Constants.TAG_SNAPSHOT_ID);
                            OutDatedDataException.checkSnapshotId(sId, localSnapshotId);
                            tempStorage.removeProperty(Constants.TAG_SNAPSHOT_ID);
                        }
                        tempStorage.removeProperty(Constants.TAG_HASH);
                        returnStorage.add(tempStorage);
                    }
                }
                finally
                {
                    objs.close();
                }
            }
        }
        finally
        {
            sess.close();
        }

        return returnStorage;
    }

    /**
     * Creates a RelationshipStorage from the edgeId.
     * @param graph the graph.
     * @param edgeId the edgeId.
     * @return the relationshipStorage.
     */
    private RelationshipStorage getRelationshipFromRelationshipId(final Graph graph, final long edgeId)
    {
        final Iterator<Integer> attributes = graph.getAttributes(edgeId).iterator();
        final Map<String, Object> localProperties = new HashMap<>();
        while (attributes.hasNext())
        {
            final int attributeId = attributes.next();
            final String attributeKey = graph.getAttribute(attributeId).getName();
            final Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(edgeId, attributeId));
            localProperties.put(attributeKey, attributeValue);
        }

        final NodeStorage tempStartNode = getNodeFromNodeId(graph, graph.getEdgeData(edgeId).getHead());
        final NodeStorage tempEndNode = getNodeFromNodeId(graph, graph.getEdgeData(edgeId).getTail());

        return new RelationshipStorage(graph.getType(graph.getObjectType(edgeId)).getName(), localProperties, tempStartNode, tempEndNode);
    }

    /**
     * Creates a NodeStorage from the nodeId.
     * @param graph the graph.
     * @param nodeId the nodeId.
     * @return the nodeStorage.
     */
    private NodeStorage getNodeFromNodeId(final Graph graph, final long nodeId)
    {
        final AttributeListIterator attributes = graph.getAttributes(nodeId).iterator();
        final Map<String, Object> localProperties = new HashMap<>();
        while (attributes.hasNext())
        {
            final int attributeId = attributes.next();
            final String attributeKey = graph.getAttribute(attributeId).getName();
            final Object attributeValue = SparkseeUtils.getObjectFromValue(graph.getAttribute(nodeId, attributeId));
            localProperties.put(attributeKey, attributeValue);
        }

        return new NodeStorage(graph.getType(graph.getObjectType(nodeId)).getName(), localProperties);
    }

    @Override
    public boolean shouldFollow(final int sequence)
    {
        if(sequence >= 3 && sequence < 6)
        {
            return false;
        }
        return true;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        final Session sess = db.newSession();
        final Graph graph = sess.getGraph();
        final NodeStorage startNode = storage.getStartNode();
        final NodeStorage endNode = storage.getEndNode();

        final List<Long> objsStart = loadNodesAsIdList(graph, startNode);
        final List<Long> objsEnd = loadNodesAsIdList(graph, endNode);

        if (objsStart.isEmpty() || objsEnd.isEmpty())
        {
            sess.close();
            return false;
        }

        final int relationshipTypeId = graph.findType(storage.getId());
        try
        {
            long start = objsStart.get(0);
            long end = objsEnd.get(0);

            int checkedIndex = 1;
            while(objsEnd.size() >= checkedIndex)
            {
                if(graph.getObjectType(start) != Type.getNodesType())
                {
                    start = objsStart.get(checkedIndex);
                }

                if(graph.getObjectType(end) != Type.getNodesType())
                {
                    end = objsStart.get(checkedIndex);
                }

                if(graph.getObjectType(end) == Type.getNodesType() && graph.getObjectType(start) == Type.getNodesType())
                {
                    break;
                }
                checkedIndex++;
            }

            if (start <= 0 || end <= 0)
            {
                sess.close();
                return false;
            }

            long oId = graph.findEdge(relationshipTypeId, start, end);
            if (oId == 0)
                return false;
            return HashCreator.sha1FromRelationship(storage).equals(graph.getAttribute(oId, graph.findAttribute(Type.getGlobalType(), "hash")).getString());
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute SHA1 for node", e);
        }
        catch (RuntimeException e)
        {
            Log.getLogger().warn("Couldn't execute the query, return false at sparksee");
        }
        finally
        {
            sess.close();
        }
        return false;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        final Session sess = db.newSession();
        final Graph graph = sess.getGraph();

        final Objects objs = findNode(graph, storage);

        if (objs == null || objs.isEmpty())
        {
            if(objs != null)
            {
                objs.close();
            }
            sess.close();
            return false;
        }

        final ObjectsIterator it = objs.iterator();

        try
        {
            final long oId = it.next();
            return HashCreator.sha1FromNode(storage).equals(graph.getAttribute(oId, graph.findAttribute(Type.getGlobalType(), "hash")).getString());
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.getLogger().warn("Couldn't execute SHA1 for node", e);
        }
        finally
        {
            objs.close();
            it.close();
            sess.close();
        }
        return false;
    }


    private List<String> reorderKeysToPutIdxFirst(Map<String, Object> map)
    {
        List<String> keys = new ArrayList<>(map.keySet());
        if (keys.isEmpty())
            return keys;

        if (keys.get(0).equals("idx"))
            return keys;

        int idx = keys.indexOf("idx");
        keys.remove(idx);
        keys.add(0, "idx");
        return  keys;
    }

    /**
     * Return a Objects array matching the nodeType and properties.
     * @param graph the graph.
     * @param storage the storage of the node.
     * @return Objects which match the attributes.
     */
    private Objects findNode(final Graph graph, final NodeStorage storage)
    {
        Objects objects = null;

        if (storage.getProperties().size() > 0) {
            final List<String> keys = reorderKeysToPutIdxFirst(storage.getProperties());
            for (final String key : keys)
            {
                final int attributeId = graph.findAttribute(Type.GlobalType, key);

                if(attributeId == Attribute.InvalidAttribute)
                {
                    continue;
                }

                if (objects == null || objects.isEmpty())
                {
                    if(objects != null)
                    {
                        objects.close();
                    }
                    objects = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(storage.getProperties().get(key)));
                }
                else
                {
                    final Objects tempObjects = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(storage.getProperties().get(key)), objects);
                    objects.close();
                    objects = tempObjects;
                }
            }
        }
        else
        {
            int nodeTypeId = SparkseeUtils.createOrFindNodeType(storage, graph);
            objects = graph.select(nodeTypeId);
        }

        //TODO need to filter Objects by nodeTypeId...
        /*if(!storage.getId().isEmpty())
        {
            int nodeTypeId = SparkseeUtils.createOrFindNodeType(storage, graph);
            Instrumentation.s("graph.select(nodeTypeId)");
            ObjectsIterator it = objects.iterator();
            while (it.hasNext())
            {
                it.next()
            }

            Instrumentation.f("graph.select(nodeTypeId)");
        }*/

        return objects;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        List<Long> objs = loadNodesAsIdList(graph, key);

        for (long nodeId : objs)
        {
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
                sess.close();
                return false;
            }

            int attributeTypeIdSnapshotId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, 1L, Type.getGlobalType(), graph);
            graph.setAttribute(nodeId, attributeTypeIdSnapshotId, SparkseeUtils.getValue(snapshotId));
        }

        sess.close();
        return false;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        int nodeTypeId = SparkseeUtils.createOrFindNodeType(storage, graph);
        long nodeId = graph.newNode(nodeTypeId);

        for(final Map.Entry<String, Object> entry : storage.getProperties().entrySet())
        {
            final int attributeId = SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.GlobalType, graph);
            graph.setAttribute(nodeId, attributeId, SparkseeUtils.getValue(entry.getValue()));
        }

        final int snapshotAttributeId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, snapshotId, Type.GlobalType, graph);
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

        Log.getLogger().info("Successfully executed create node transaction in server:  " + id);
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
        Log.getLogger().info("Successfully executed delete node transaction in server:  " + id);
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();
        List<Long> startObjs = loadNodesAsIdList(graph, key.getStartNode());
        List<Long> endObjs = loadNodesAsIdList(graph, key.getStartNode());
        for (long startNode : startObjs)
        {
            for (long endNode : endObjs)
            {
                int edgeType = graph.findType(key.getId());
                if (edgeType == Type.InvalidType)
                    continue;
                long relationship = graph.findEdge(edgeType, startNode, endNode);
                if (relationship == 0)
                    continue;
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
                    sess.close();
                    return false;
                }
                int attributeTypeIdSnapshotId = SparkseeUtils.createOrFindAttributeType(Constants.TAG_SNAPSHOT_ID, 1L, Type.getGlobalType(), graph);
                graph.setAttribute(relationship, attributeTypeIdSnapshotId, SparkseeUtils.getValue(snapshotId));
            }
        }
        Log.getLogger().info("Successfully executed update relationship transaction in server:  " + id);
        sess.close();
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        final Session sess = db.newSession();
        final Graph graph = sess.getGraph();
        final Objects startObjs = findNode(graph, storage.getStartNode());
        final Objects endObjs = findNode(graph, storage.getEndNode());

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

        final ObjectsIterator startIt = startObjs.iterator();
        final ObjectsIterator endIt = endObjs.iterator();

        while(startIt.hasNext())
        {
            long startNode = startIt.next();
            while (endIt.hasNext())
            {
                final long endNode = endIt.next();

                int edgeType = graph.findType(storage.getId());
                if (Type.InvalidType == edgeType)
                {
                    edgeType = graph.newEdgeType(storage.getId(), true, false);
                }

                final long relationship = graph.findOrCreateEdge(edgeType, startNode, endNode);
                for (final Map.Entry<String, Object> entry : storage.getProperties().entrySet())
                {
                    graph.setAttribute(relationship,
                            SparkseeUtils.createOrFindAttributeType(entry.getKey(), entry.getValue(), Type.GlobalType, graph),
                            SparkseeUtils.getValue(entry.getValue()));
                }

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
                    endObjs.close();
                    startObjs.close();
                    startIt.close();
                    endIt.close();
                    sess.close();
                    return false;
                }
                Log.getLogger().info("Successfully executed create relationship transaction in server:  " + id);
            }
        }

        startObjs.close();
        endObjs.close();
        startIt.close();
        endIt.close();
        sess.close();
        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        final Session sess = db.newSession();
        final Graph graph = sess.getGraph();
        final List<Long> startObjs = loadNodesAsIdList(graph, storage.getStartNode());
        final List<Long> endObjs = loadNodesAsIdList(graph, storage.getStartNode());

        for (long startNode : startObjs)
        {
            for (long endNode : endObjs)
            {
                int edgeType = graph.findType(storage.getId());
                if (edgeType == Type.InvalidType)
                    continue;

                long edgeId = graph.findEdge(edgeType, startNode, endNode);
                if (edgeId != 0)
                    graph.drop(edgeId);
            }
        }
        sess.close();
        return true;
    }
}
