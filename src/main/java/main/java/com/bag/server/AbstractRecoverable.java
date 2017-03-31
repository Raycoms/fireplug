package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.OrientDBDatabaseAccess;
import main.java.com.bag.server.database.SparkseeDatabaseAccess;
import main.java.com.bag.server.database.TitanDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.TransactionStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Super class of local or global cluster.
 * Used for common communication methods.
 */
public abstract class AbstractRecoverable extends DefaultRecoverable
{
    /**
     * Contains the local server replica.
     */
    private ServiceReplica replica = null;

    /**
     * Global snapshot id, increases with every committed transaction.
     */
    private long globalSnapshotId = 0;

    /**
     * Unique Id of the server.
     */
    private int id;

    /**
     * Write set of the nodes contains updates and deletes.
     */
    private TreeMap<Long, List<Operation>> globalWriteSet;

    /**
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    private final ServerWrapper wrapper;

    private KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        // configure kryo instance, customize settings
        return kryo;
    };

    protected AbstractRecoverable(int id, final String configDirectory, final ServerWrapper wrapper)
    {
        this.id = id;
        this.wrapper = wrapper;
        globalSnapshotId = 1;
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        //the default verifier is instantiated with null in the ServerReplica.
        this.replica = new ServiceReplica(id, configDirectory, this, this, null, new DefaultReplier());

        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        pool.release(kryo);

        globalWriteSet = new TreeMap<>();
    }

    @Override
    public void installSnapshot(final byte[] bytes)
    {
        if (bytes == null)
        {
            return;
        }
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();

        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);

        globalSnapshotId = kryo.readObject(input, Long.class);

        if (input.canReadInt())
        {
            int writeSetSize = kryo.readObject(input, Integer.class);

            for (int i = 0; i < (Integer) writeSetSize; i++)
            {
                long snapshotId = kryo.readObject(input, Long.class);
                Object object = kryo.readClassAndObject(input);
                if (object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof Operation)
                {
                    globalWriteSet.put(snapshotId, (List<Operation>) object);
                }
            }
        }

        this.id = kryo.readObject(input, Integer.class);
        String instance = kryo.readObject(input, String.class);
        wrapper.instantiateDBAccess(instance, wrapper.getGlobalServerId());

        readSpecificData(input, kryo);

        this.replica = new ServiceReplica(id, this, this);
        this.replica.setReplyController(new DefaultReplier());

        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);

        input.close();
        pool.release(kryo);
    }

    /**
     * Read the specific data of a local or global cluster.
     * @param input object to read from.
     * @param kryo kryo object.
     */
    abstract void readSpecificData(final Input input, final Kryo kryo);

    /**
     * Write the specific data of a local or global cluster.
     * @param output object to write to.
     * @param kryo kryo object.
     */
    abstract void writeSpecificData(final Output output, final Kryo kryo);

    @Override
    public byte[] getSnapshot()
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        Output output = new Output(0, 100240);

        kryo.writeObject(output, globalSnapshotId);

        if (globalWriteSet == null)
        {
            globalWriteSet = new TreeMap<>();
        }
        else
        {
            kryo.writeObject(output, globalWriteSet.size());
            for (Map.Entry<Long, List<Operation>> writeSet : globalWriteSet.entrySet())
            {
                kryo.writeObject(output, writeSet.getKey());
                kryo.writeObject(output, writeSet.getValue());
            }
        }

        kryo.writeObject(output, id);
        IDatabaseAccess databaseAccess = wrapper.getDataBaseAccess();
        if (databaseAccess instanceof Neo4jDatabaseAccess)
        {
            kryo.writeObject(output, Constants.NEO4J);
        }
        else if (databaseAccess instanceof TitanDatabaseAccess)
        {
            kryo.writeObject(output, Constants.TITAN);
        }
        else if (databaseAccess instanceof OrientDBDatabaseAccess)
        {
            kryo.writeObject(output, Constants.ORIENTDB);
        }
        else if (databaseAccess instanceof SparkseeDatabaseAccess)
        {
            kryo.writeObject(output, Constants.SPARKSEE);
        }
        else
        {
            kryo.writeObject(output, "none");
        }

        writeSpecificData(output, kryo);

        byte[] bytes = output.getBuffer();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Handles the node read message and requests it to the database.
     *
     * @param input          get info from.
     * @param messageContext additional context.
     * @param kryo           kryo object.
     * @param output         write info to.
     * @return output object to return to client.
     */
    public Output handleNodeRead(Input input, MessageContext messageContext, Kryo kryo, Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        NodeStorage identifier = kryo.readObject(input, NodeStorage.class);
        input.close();

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetNodes(identifier);
            //localTransactionList.put(messageContext.getSender(), transaction);
            localSnapshotId = getGlobalSnapshotId();
        }
        ArrayList<Object> returnList = null;

        Log.getLogger().info("Get info from databaseAccess to snapShotId " + localSnapshotId);

        try
        {
            returnList = new ArrayList<>(wrapper.getDataBaseAccess().readObject(identifier, localSnapshotId));
        }
        catch (OutDatedDataException e)
        {
            Log.getLogger().info("Transaction found conflict - terminating", e);
            terminate();
            return output;
        }

        if (returnList != null)
        {
            Log.getLogger().info("Got info from databaseAccess: " + returnList.size());
        }

        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeObject(output, new ArrayList<NodeStorage>());
            kryo.writeObject(output, new ArrayList<RelationshipStorage>());
            return output;
        }

        ArrayList<NodeStorage> nodeStorage = new ArrayList<>();
        ArrayList<RelationshipStorage> relationshipStorage = new ArrayList<>();
        for (Object obj : returnList)
        {
            if (obj instanceof NodeStorage)
            {
                nodeStorage.add((NodeStorage) obj);
            }
            else if (obj instanceof RelationshipStorage)
            {
                relationshipStorage.add((RelationshipStorage) obj);
            }
        }

        kryo.writeObject(output, nodeStorage);
        kryo.writeObject(output, relationshipStorage);

        return output;
    }

    /**
     * Handles the relationship read message and requests it to the database.
     *
     * @param input          get info from.
     * @param messageContext additional context.
     * @param kryo           kryo object.
     * @param output         write info to.
     * @return output object to return to client.
     */
    public Output handleRelationshipRead(final Input input, final MessageContext messageContext, final Kryo kryo, final Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        RelationshipStorage identifier = kryo.readObject(input, RelationshipStorage.class);
        input.close();

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetRelationship(identifier);
            //localTransactionList.put(messageContext.getSender(), transaction);
            localSnapshotId = getGlobalSnapshotId();
        }
        ArrayList<Object> returnList = null;


        Log.getLogger().info("Get info from databaseAccess");
        try
        {
            returnList = new ArrayList<>((wrapper.getDataBaseAccess()).readObject(identifier, localSnapshotId));
        }
        catch (OutDatedDataException e)
        {
            Log.getLogger().info("Transaction found conflict - terminating", e);
            terminate();
        }

        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeObject(output, new ArrayList<NodeStorage>());
            kryo.writeObject(output, new ArrayList<RelationshipStorage>());
            return output;
        }
        Log.getLogger().info("Got info from databaseAccess: " + returnList.size());

        ArrayList<NodeStorage> nodeStorage = new ArrayList<>();
        ArrayList<RelationshipStorage> relationshipStorage = new ArrayList<>();
        for (Object obj : returnList)
        {
            if (obj instanceof NodeStorage)
            {
                nodeStorage.add((NodeStorage) obj);
            }
            else if (obj instanceof RelationshipStorage)
            {
                relationshipStorage.add((RelationshipStorage) obj);
            }
        }

        //todo problem returning the relationship here!
        kryo.writeObject(output, nodeStorage);
        kryo.writeObject(output, relationshipStorage);

        return output;
    }

    /**
     * Execute the commit on the replica.
     * @param localWriteSet the write set to execute.
     */
    public synchronized long executeCommit(final List<Operation> localWriteSet)
    {
        ++globalSnapshotId;
        //Execute the transaction.
        for (Operation op : localWriteSet)
        {
            op.apply(wrapper.getDataBaseAccess(), globalSnapshotId);
        }
        this.globalWriteSet.put(getGlobalSnapshotId(), localWriteSet);

        return globalSnapshotId;
    }

    /**
     * Getter for the service replica.
     * @return instance of the service replica
     */
    public ServiceReplica getReplica()
    {
        return replica;
    }

    /**
     * Getter for the global snapshotId.
     * @return the snapshot id.
     */
    public long getGlobalSnapshotId()
    {
        return globalSnapshotId;
    }

    /**
     * Get a copy of the global writeSet.
     * @return a hashmap of all the operations with their snapshotId.
     */
    public TreeMap<Long, List<Operation>> getGlobalWriteSet()
    {
        return new TreeMap<>(globalWriteSet);
    }

    /**
     * Shuts down the Server.
     */
    public void terminate()
    {
        this.replica.kill();
    }

    /**
     * Get the kryoFactory of this recoverable.
     * @return the factory.
     */
    public KryoFactory getFactory()
    {
        return factory;
    }
}