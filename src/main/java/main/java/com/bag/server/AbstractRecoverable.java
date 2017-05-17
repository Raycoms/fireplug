package main.java.com.bag.server;

import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.github.benmanes.caffeine.cache.*;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.TransactionStorage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Super class of local or global cluster.
 * Used for common communication methods.
 */
public abstract class AbstractRecoverable extends DefaultRecoverable
{

    /**
     * Keep the last x transaction in a separate list.
     */
    public static final int KEEP_LAST_X = 50;

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
    private ConcurrentSkipListMap<Long, List<IOperation>> globalWriteSet;

    /**
     * Write set cache of the nodes contains updates and deletes but only of the last x transactions.
     */
    private Cache<Long, List<IOperation>> latestWritesSet = Caffeine.newBuilder()
            .maximumSize(KEEP_LAST_X)
            .writer(new CacheWriter<Long, List<IOperation>>()
            {
                @Override
                public void write(@NotNull final Long key, @NotNull final List<IOperation> value)
                {
                    //Nothing to do.
                }

                @Override
                public void delete(@NotNull final Long key, @Nullable final List<IOperation> value, @NotNull final RemovalCause cause)
                {
                    globalWriteSet.put(key, value);
                }
            }).build();

    /**
     * Object to lock on commits.
     */
    private final Object commitLock = new Object();

    /**
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    private final ServerWrapper wrapper;

    private final ServerInstrumentation instrumentation;

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

    /**
     * Creates an instance of the abstract recoverable.
     *
     * @param id              with the id.
     * @param configDirectory the config directory.
     * @param wrapper         the overlying wrapper class.
     * @param instrumentation the instrumentation for evaluation.
     */
    protected AbstractRecoverable(final int id, final String configDirectory, final ServerWrapper wrapper, final ServerInstrumentation instrumentation)
    {
        this.id = id;
        this.wrapper = wrapper;
        this.instrumentation = instrumentation;
        globalSnapshotId = 1;
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        Log.getLogger().warn("Instantiating abstract recoverable of id: " + id);
        //the default verifier is instantiated with null in the ServerReplica.
        this.replica = new ServiceReplica(id, configDirectory, this, this, null, new DefaultReplier());
        Log.getLogger().warn("Finished instantiating abstract recoverable of id: " + id);
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        pool.release(kryo);

        globalWriteSet = new ConcurrentSkipListMap<>();

        Log.getLogger().warn("Instantiating fileWriter.");
        try (final FileWriter file = new FileWriter(System.getProperty("user.home") + "/results" + id + ".txt", true);
             final BufferedWriter bw = new BufferedWriter(file);
             final PrintWriter out = new PrintWriter(bw))
        {
            out.println();
            out.println("Starting new experiment: ");
            out.println();
            out.print("time;");
            out.print("aborts;");
            out.print("commits;");
            out.print("reads;");
            out.print("writes;");
            out.print("throughput");
            out.println();
        }
        catch (IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
        Log.getLogger().warn("Finished file writer instantiation.");
    }

    /**
     * Check for conflicts and unpack things for conflict handle check.
     *
     * @param kryo  the kryo instance.
     * @param input the input.
     * @return the response.
     */
    public byte[] executeReadOnlyCommit(final Kryo kryo, final Input input, final long timeStamp)
    {
        //Read the inputStream.
        final List readsSetNodeX = kryo.readObject(input, ArrayList.class);
        final List readsSetRelationshipX = kryo.readObject(input, ArrayList.class);
        final List writeSetX = kryo.readObject(input, ArrayList.class);

        //Create placeHolders.
        final ArrayList<NodeStorage> readSetNode;
        final ArrayList<RelationshipStorage> readsSetRelationship;
        final ArrayList<IOperation> localWriteSet;

        input.close();
        final Output output = new Output(128);
        kryo.writeObject(output, Constants.COMMIT_RESPONSE);

        try
        {
            readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
            readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            localWriteSet = (ArrayList<IOperation>) writeSetX;
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't convert received data to sets. Returning abort", e);
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        Log.getLogger().info("Search conflicts for readOnly commit with snapshot id: " + timeStamp);
        if (!ConflictHandler.checkForConflict(getGlobalWriteSet(),
                getLatestWritesSet(),
                localWriteSet,
                readSetNode,
                readsSetRelationship,
                timeStamp,
                wrapper.getDataBaseAccess()))
        {
            updateCounts(0, 0, 0, 1);

            Log.getLogger()
                    .warn("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                            + localWriteSet.size()
                            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        updateCounts(0, 0, 1, 0);

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length + " and local id: " + timeStamp);

        return returnBytes;
    }

    /**
     * Handle a read only commit.
     *
     * @param input the data.
     * @param kryo  kryo instance.
     * @return the response data.
     */
    public byte[] handleReadOnlyCommit(final Input input, final Kryo kryo)
    {
        final Long timeStamp = kryo.readObject(input, Long.class);
        return executeReadOnlyCommit(kryo, input, timeStamp);
    }

    public void updateCounts(int writes, int reads, int commits, int aborts)
    {
        instrumentation.updateCounts(writes, reads, commits, aborts);
    }

    @Override
    public void installSnapshot(final byte[] bytes)
    {
        Log.getLogger().warn("Install snapshot!");
        if (bytes == null || bytes.length <= 1)
        {
            return;
        }
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();

        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);

        globalSnapshotId = kryo.readObject(input, Long.class);
        Log.getLogger().error("Install snapshot with old values!!!!: " + globalSnapshotId);
        globalWriteSet.clear();

        while (input.canReadLong())
        {
            long snapshotId = kryo.readObject(input, Long.class);
            Object object = kryo.readClassAndObject(input);
            if (object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof IOperation)
            {
                globalWriteSet.put(snapshotId, (List<IOperation>) object);
            }
        }

        latestWritesSet.cleanUp();
        while (input.canReadLong())
        {
            long snapshotId = kryo.readObject(input, Long.class);
            Object object = kryo.readClassAndObject(input);
            if (object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof IOperation)
            {
                latestWritesSet.put(snapshotId, (List<IOperation>) object);
            }
        }

        this.id = kryo.readObject(input, Integer.class);
        String instance = kryo.readObject(input, String.class);
        wrapper.setDataBaseAccess(ServerWrapper.instantiateDBAccess(instance, wrapper.getGlobalServerId()));

        readSpecificData(input, kryo);

        this.replica = new ServiceReplica(id, this, this);
        this.replica.setReplyController(new DefaultReplier());

        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);

        input.close();
        pool.release(kryo);
    }

    /**
     * Read the specific data of a local or global cluster.
     *
     * @param input object to read from.
     * @param kryo  kryo object.
     */
    abstract void readSpecificData(final Input input, final Kryo kryo);

    /**
     * Write the specific data of a local or global cluster.
     *
     * @param output object to write to.
     * @param kryo   kryo object.
     */
    abstract Output writeSpecificData(final Output output, final Kryo kryo);

    @Override
    public byte[] getSnapshot()
    {
        if (globalWriteSet == null || latestWritesSet == null)
        {
            return new byte[] {0};
        }
        Log.getLogger().warn("Snapshot!");
        //Log.getLogger().warn("Get snapshot!!: " + globalWriteSet.size() + " + " + latestWritesSet.estimatedSize());
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        Output output = new Output(0, 600240);
        kryo.writeObject(output, getGlobalSnapshotId());

        for (final Map.Entry<Long, List<IOperation>> writeSet : globalWriteSet.entrySet())
        {
            kryo.writeObject(output, writeSet.getKey());
            kryo.writeObject(output, writeSet.getValue());
        }

        for (final Map.Entry<Long, List<IOperation>> writeSet : latestWritesSet.asMap().entrySet())
        {
            kryo.writeObject(output, writeSet.getKey());
            kryo.writeObject(output, writeSet.getValue());
        }

        kryo.writeObject(output, id);
        IDatabaseAccess databaseAccess = wrapper.getDataBaseAccess();
        kryo.writeObject(output, databaseAccess.toString());

        output = writeSpecificData(output, kryo);

        Log.getLogger().warn("Finished snapshotting");
        byte[] bytes = output.getBuffer();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Handles the node read message and requests it to the database.
     *
     * @param input  get info from.
     * @param kryo   kryo object.
     * @param output write info to.
     * @return output object to return to client.
     */
    Output handleNodeRead(final Input input, final Kryo kryo, final Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        NodeStorage identifier = kryo.readObject(input, NodeStorage.class);
        input.close();

        updateCounts(0, 1, 0, 0);

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
        catch (final OutDatedDataException e)
        {
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, localSnapshotId);

            Log.getLogger().warn("Transaction found conflict");
            Log.getLogger().info("OutdatedData Exception thrown: ", e);
            kryo.writeObject(output, new ArrayList<NodeStorage>());
            kryo.writeObject(output, new ArrayList<RelationshipStorage>());
            return output;
        }

        kryo.writeObject(output, Constants.CONTINUE);
        kryo.writeObject(output, localSnapshotId);


        Log.getLogger().info("Got info from databaseAccess: " + returnList.size());

        if (returnList.isEmpty())
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
     * @param input  get info from.
     * @param kryo   kryo object.
     * @param output write info to.
     * @return output object to return to client.
     */
    Output handleRelationshipRead(final Input input, final Kryo kryo, final Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        RelationshipStorage identifier = kryo.readObject(input, RelationshipStorage.class);
        input.close();

        updateCounts(0, 1, 0, 0);

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
        catch (final OutDatedDataException e)
        {
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, localSnapshotId);

            Log.getLogger().warn("Transaction found conflict");
            Log.getLogger().info("OutdatedData Exception thrown: ", e);
            kryo.writeObject(output, new ArrayList<NodeStorage>());
            kryo.writeObject(output, new ArrayList<RelationshipStorage>());
        }
        kryo.writeObject(output, Constants.CONTINUE);
        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeObject(output, new ArrayList<NodeStorage>());
            kryo.writeObject(output, new ArrayList<RelationshipStorage>());
            return output;
        }
        Log.getLogger().info("Got info from databaseAccess: " + returnList.size());

        final ArrayList<NodeStorage> nodeStorage = new ArrayList<>();
        final ArrayList<RelationshipStorage> relationshipStorage = new ArrayList<>();
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
     *
     * @param localWriteSet the write set to execute.
     */
    void executeCommit(final List<IOperation> localWriteSet)
    {
        synchronized (commitLock)
        {
            final long currentSnapshot = ++globalSnapshotId;
            //Execute the transaction.
            for (IOperation op : localWriteSet)
            {
                boolean success;
                do
                {
                    success = op.apply(wrapper.getDataBaseAccess(), globalSnapshotId);
                }
                while(!success);
                //Log.getLogger().warn(currentSnapshot + " Running on: " + location + " op: " + op.toString());


                updateCounts(1, 0, 0, 0);
            }
            this.putIntoWriteSet(currentSnapshot, localWriteSet);
        }

        updateCounts(0, 0, 1, 0);
    }

    /**
     * Put certain writes into the writeSet.
     *
     * @param currentSnapshot the currentSnapshot.
     * @param localWriteSet   the local writeset.
     */
    public void putIntoWriteSet(final long currentSnapshot, final List<IOperation> localWriteSet)
    {
        latestWritesSet.put(currentSnapshot, localWriteSet);
    }

    /**
     * Getter for the service replica.
     *
     * @return instance of the service replica
     */
    ServiceReplica getReplica()
    {
        return replica;
    }

    /**
     * Getter for the global snapshotId.
     *
     * @return the snapshot id.
     */
    long getGlobalSnapshotId()
    {
        return globalSnapshotId;
    }

    /**
     * Get a copy of the global writeSet.
     *
     * @return a hashmap of all the operations with their snapshotId.
     */
    ConcurrentSkipListMap<Long, List<IOperation>> getGlobalWriteSet()
    {
        return globalWriteSet;
    }

    /**
     * Get a copy of the global writeSet.
     *
     * @return a hashmap of all the operations with their snapshotId.
     */
    Map<Long, List<IOperation>> getLatestWritesSet()
    {
        return latestWritesSet.asMap();
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
     *
     * @return the factory.
     */
    KryoFactory getFactory()
    {
        return factory;
    }

    /**
     * Getter for the id.
     *
     * @return the id.
     */
    int getId()
    {
        return id;
    }

    /**
     * Set the global snapshot id.
     *
     * @param globalSnapshotId the id to set.
     */
    public void setGlobalSnapshotId(final long globalSnapshotId)
    {
        this.globalSnapshotId = globalSnapshotId;
    }
}
