package main.java.com.bag.server.database;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.sparsity.sparksee.gdb.*;
import com.sparsity.sparksee.gdb.Objects;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.HashCreator;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;

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
            Log.getLogger().info(Long.toString(snapshotId));
            builder.append(buildRelationshipString(relationshipStorage));
            builder.append(" RETURN r");
        }
        else
        {
            Log.getLogger().info(Long.toString(snapshotId));
            builder.append(buildNodeString(nodeStorage, ""));
            builder.append(" RETURN n");
        }

        sess.close();

        return returnStorage;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();


        int nodeType = graph.findType(storage.getId());
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
                return false;
            }
        }

        if (objs == null || objs.isEmpty())
        {
            return false;
        }

        ObjectsIterator it = objs.iterator();

        try
        {
            long oId = it.next();
            return HashCreator.sha1FromRelationship(storage).equals(graph.getAttribute(oId, graph.findAttribute(nodeType, "hash")).getString());
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

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        int nodeType = graph.findType(storage.getId());
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
                return false;
            }
        }

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
