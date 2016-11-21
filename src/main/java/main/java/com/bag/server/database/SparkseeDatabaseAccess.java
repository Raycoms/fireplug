package main.java.com.bag.server.database;

import com.sparsity.sparksee.gdb.*;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.HashCreator;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.FileNotFoundException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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


        // Use 'graph' to perform operations on the graph database
        sess.close();

        return null;
    }

    @Override
    public boolean equalHash(final List readSet)
    {
        return false;
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        return false;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        Session sess = db.newSession();
        Graph graph = sess.getGraph();

        int nodeType = graph.findType(storage.getId());
        Objects objs = null;

        for(Map.Entry<String, Object> entry : storage.getProperties().entrySet())
        {
            int attributeId = graph.findAttribute(nodeType, entry.getKey());

            if(objs == null)
            {
                objs = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(entry.getValue()));
            }
            else
            {
                objs = graph.select(attributeId, Condition.Equal, SparkseeUtils.getValue(entry.getValue()), objs);
            }

            if(objs.isEmpty())
            {
                return false;
            }
        }

        if(objs == null || objs.isEmpty())
        {
            return false;
        }

        Iterator it = objs.iterator();

            try
            {
                //if(it.next()
                if (!HashCreator.sha1FromNode(storage).equals(null))//.equals(n.getProperty(Constants.TAG_HASH)))
                {
                    sess.close();
                    return false;
                }
            }
            catch (NoSuchAlgorithmException e)
            {
                Log.getLogger().warn("Couldn't execute SHA1 for node", e);
            }



        //Objects people  = graph.select(nameAttrId, Condition.Equal, v.setString("Carol"));
        //Assuming we only get one node in return.
        /*if (result.hasNext())
        {
            Map<String, Object> value = result.next();
            for (Map.Entry<String, Object> entry : value.entrySet())
            {
                if (entry.getValue() instanceof NodeProxy)
                {
                    NodeProxy n = (NodeProxy) entry.getValue();

                    try
                    {
                        if (!HashCreator.sha1FromNode(storage).equals(n.getProperty(Constants.TAG_HASH)))
                        {
                            sess.close();
                            return false;
                        }
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.getLogger().warn("Couldn't execute SHA1 for node", e);
                    }
                    break;
                }
            }
        }*/

        sess.close();
        return true;
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
