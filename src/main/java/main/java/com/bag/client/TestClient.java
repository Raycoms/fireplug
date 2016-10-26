package main.java.com.bag.client;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.util.Extractor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Class handling the client.
 */
public class TestClient extends ServiceProxy
{
    /**
     * Should the transaction run in secure mode?
     */
    private boolean secureMode = false;
    /**
     * Sets to log reads, updates, deletes and node creations.
     */
    private HashMap<NodeStorage, NodeStorage> readsSetNode;
    private HashMap<NodeStorage, NodeStorage> updateSetNode;
    private ArrayList<NodeStorage>            deleteSetNode;
    private ArrayList<NodeStorage>            createSetNode;

    /**
     * Sets to log reads, updates, deletes and relationship creations.
     */
    private HashMap<RelationshipStorage, RelationshipStorage> readsSetRelationship;
    private HashMap<RelationshipStorage, RelationshipStorage> updateSetRelationship;
    private ArrayList<RelationshipStorage>                    deleteSetRelationship;
    private ArrayList<RelationshipStorage>                    createSetRelationship;

    /**
     * Local timestamp of the current transaction.
     */
    private long localTimeStamp = 0;

    /**
     * Create a threadsafe version of kryo.
     */
    private KryoFactory factory = new KryoFactory()
    {
        public Kryo create ()
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            // configure kryo instance, customize settings
            return kryo;
        }
    };

    public TestClient(final int processId)
    {
        super(processId);
        secureMode = false;
        initClient();
    }

    public TestClient(final int processId, final String configHome)
    {
        super(processId, configHome);
        initClient();
    }

    public TestClient(final int processId, final String configHome, final Comparator<byte[]> replyComparator, final Extractor replyExtractor)
    {
        super(processId, configHome, replyComparator, replyExtractor);
        initClient();
    }

    /**
     * Initiates the client maps and registers necessary operations.
     */
    private void initClient()
    {
        readsSetNode = new HashMap<NodeStorage, NodeStorage>();
        updateSetNode = new HashMap<NodeStorage, NodeStorage>();
        deleteSetNode = new ArrayList<NodeStorage>();
        createSetNode = new ArrayList<NodeStorage>();

        readsSetRelationship = new HashMap<RelationshipStorage, RelationshipStorage>();
        updateSetRelationship = new HashMap<RelationshipStorage, RelationshipStorage>();
        deleteSetRelationship = new ArrayList<RelationshipStorage>();
        createSetRelationship = new ArrayList<RelationshipStorage>();
    }

    /**
     * Write requests. (Only reach database on commit)
     */
    public void write()
    {

    }

    //todo may add a list of identifier here.
    /**
     * ReadRequests.(Directly read database)
     * @param identifier, object which should be read, may be NodeStorage or RelationshipStorage
     */
    public void read(Object identifier)
    {
        if(identifier instanceof NodeStorage)
        {
            invokeUnordered(serialize("node/read", localTimeStamp, identifier));
            return;
        }
        else if(identifier instanceof RelationshipStorage)
        {
            invokeUnordered(serialize("relationShip/read", localTimeStamp, identifier));
            return;
        }
        Log.getLogger().warn("Unsupported identifier: " + identifier.toString());
    }

    /**
     * Commit reaches the server, if secure commit send to all, else only send to one
     */
    public void commit()
    {
        boolean readOnly = isReadOnly();

        //Sample data
        readsSetNode.put(new NodeStorage("a"), new NodeStorage("e"));
        readsSetNode.put(new NodeStorage("b"), new NodeStorage("f"));
        readsSetNode.put(new NodeStorage("c"), new NodeStorage("g"));
        readsSetNode.put(new NodeStorage("d"), new NodeStorage("h"));


        byte[] bytes = serializeAll();
        if(readOnly && !secureMode)
        {
            invokeUnordered(bytes);
        }
        else
        {
            invokeOrdered(bytes);
        }
    }

    /**
     * Serializes the data and returns it in byte format.
     * @return the data in byte format.
     */
    private byte[] serialize(String reason, Object...args)
    {

        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        //Todo probably will need a bigger buffer in the future.
        Output output = new Output(0, 1024);

        kryo.writeClassAndObject(output, reason);
        for(Object identifier: args)
        {
            if(identifier instanceof NodeStorage || identifier instanceof RelationshipStorage || identifier instanceof Long)
            {
                kryo.writeClassAndObject(output, identifier);
            }
        }

        byte[] bytes = output.toBytes();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Serializes all sets and returns it in byte format.
     * @return the data in byte format.
     */
    private byte[] serializeAll()
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        //Todo probably will need a bigger buffer in the future.
        Output output = new Output(0, 1024);

        //Write the timeStamp to the server
        kryo.writeClassAndObject(output, localTimeStamp);

        //Write the node-sets to the server
        kryo.writeClassAndObject(output, readsSetNode);
        kryo.writeClassAndObject(output, updateSetNode);
        kryo.writeClassAndObject(output, deleteSetNode);
        kryo.writeClassAndObject(output, createSetNode);

        //Write the relationship-sets to the server
        kryo.writeClassAndObject(output, readsSetRelationship);
        kryo.writeClassAndObject(output, updateSetRelationship);
        kryo.writeClassAndObject(output, deleteSetRelationship);
        kryo.writeClassAndObject(output, createSetRelationship);

        byte[] bytes = output.toBytes();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Checks if the transaction has made any changes to the update sets.
     * @return true if not.
     */
    private boolean isReadOnly()
    {
        return updateSetNode.isEmpty()
                && updateSetRelationship.isEmpty()
                && deleteSetRelationship.isEmpty()
                && deleteSetNode.isEmpty()
                && createSetRelationship.isEmpty()
                && createSetNode.isEmpty();
    }
}
