package main.java.com.bag.server;

import bftsmart.reconfiguration.util.RSAKeyLoader;
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
import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.reconfiguration.sensors.LoadSensor;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Super class of local or global cluster.
 * Used for common communication methods.
 */
public abstract class AbstractRecoverable extends DefaultRecoverable
{
    /**
     * Keep the last x transaction in a separate list.
     */
    private static final int KEEP_LAST_X = 50;

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
    private final int id;

    /**
     * Lock for cleanUp.
     */
    final Lock reentrantLock = new ReentrantLock();

    /**
     * List of clients with the last snapshotId they read..
     */
    private final ConcurrentSkipListMap<Integer, Long> clients = new ConcurrentSkipListMap<>();

    /**
     * Write set of the nodes contains updates and deletes.
     */
    private final ConcurrentSkipListMap<Long, List<IOperation>> globalWriteSet;

    /**
     * The last batch which has been executed.
     */
    protected long lastBatch = 0;

    /**
     * Write set cache of the nodes contains updates and deletes but only of the last x transactions.
     */
    private final Cache<Long, List<IOperation>> latestWritesSet = Caffeine.newBuilder()
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
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    protected final ServerWrapper wrapper;

    /**
     * Server instrumentation to print the results.
     */
    private final ServerInstrumentation instrumentation;

    /**
     * Thread pool for message sending.
     */
    protected final ExecutorService service = Executors.newFixedThreadPool(10);

    /**
     * Thread pool for message sending.
     */
    protected final ExecutorService localDis = Executors.newSingleThreadExecutor();

    /**
     * The global transaction id, incremented with each write transaction!
     */
    protected long globalTransactionId = 0;

    /**
     * Factory for all Kryo related parts. Will give you a kryo object and receives it on release, to avoid memory leaks.
     */
    private final KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        kryo.register(LoadSensor.LoadDesc.class, 400);
        // configure kryo instance, customize settings
        return kryo;
    };

    /**
     * Creates an instance of the abstract recoverable.
     *  @param id              with the id.
     * @param configDirectory the config directory.
     * @param wrapper         the overlying wrapper class.
     * @param instrumentation the instrumentation for evaluation.
     * @param lastBatch
     */
    protected AbstractRecoverable(final int id, final String configDirectory, final ServerWrapper wrapper, final ServerInstrumentation instrumentation, final long lastBatch)
    {
        this.id = id;
        this.wrapper = wrapper;
        this.instrumentation = instrumentation;
        this.lastBatch = lastBatch;
        globalSnapshotId = 1;
        globalWriteSet = new ConcurrentSkipListMap<>();
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        pool.release(kryo);

        Log.getLogger().error("Instantiating abstract recoverable of id: " + id + " at config directory: " + configDirectory);
        //the default verifier is instantiated with null in the ServerReplica.
        this.replica = new ServiceReplica(id, configDirectory, this, this, null, new DefaultReplier());
        Log.getLogger().error("Finished instantiating abstract recoverable of id: " + id);

        Log.getLogger().error("Instantiating fileWriter.");
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
        catch (final IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
        Log.getLogger().error("Finished file writer instantiation.");
    }

    /**
     * Update the instrumentation counts.
     *
     * @param writes  the writes.
     * @param reads   the reads.
     * @param commits the commits.
     * @param aborts  the aborts.
     */
    public void updateCounts(final int writes, final int reads, final int commits, final int aborts)
    {
        instrumentation.updateCounts(writes, reads, commits, aborts);
    }

    @Override
    public void installSnapshot(final byte[] bytes)
    {
        /**
         * No Snapshots for the time being required.
         */
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

    /**
     * Clean up all entries in write sets which we don't need anymore since we have the guarantee that the client will only respond with a higher id.
     */
    private void shortCleanUp()
    {
        if (!clients.isEmpty() && reentrantLock.tryLock())
        {
            long smallestSnapshot = Long.MAX_VALUE;
            for (final Map.Entry<Integer, Long> entry : clients.entrySet())
            {
                if (entry.getValue() < smallestSnapshot)
                {
                    smallestSnapshot = entry.getValue();
                }
            }

            if (smallestSnapshot > 0)
            {
                final long end = globalWriteSet.firstKey();
                Log.getLogger().info("Global size; " + globalWriteSet.size() + " deleting from: " + smallestSnapshot + " to: " + end);

                for (long i = end; i < smallestSnapshot; i++)
                {
                    globalWriteSet.remove(i);
                }
                Log.getLogger().info("Global size now; " + globalWriteSet.size());
            }
            reentrantLock.unlock();
        }
    }

    @Override
    public byte[] getSnapshot()
    {
        /**
         * No Snapshots for the time being required.
         */
        return new byte[] {0, 1};
    }

    /**
     * Handles the node read message and requests it to the database.
     *
     * @param input    get info from.
     * @param kryo     kryo object.
     * @param output   write info to.
     * @param clientId the id of the reading client.
     * @return output object to return to client.
     */
    public Output handleNodeRead(final Input input, final Kryo kryo, final Output output, final int clientId)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        final NodeStorage identifier = kryo.readObject(input, NodeStorage.class);
        input.close();

        updateCounts(0, 1, 0, 0);

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            //Transaction Storage is not used for now.
            /**TransactionStorage transaction = new TransactionStorage();
             transaction.addReadSetNodes(identifier);
             localTransactionList.put(messageContext.getSender(), transaction);
             **/

            localSnapshotId = getGlobalSnapshotId();
            if (!clients.containsKey(clientId) || clients.get(clientId) < localSnapshotId)
            {
                clients.put(clientId, localSnapshotId);
                if (localSnapshotId % 1000 == 0)
                {
                    shortCleanUp();
                }
            }
        }
        ArrayList<Object> returnList = null;

        Log.getLogger().info("Get info from databaseAccess to snapShotId " + localSnapshotId);

        if(!identifier.getId().equalsIgnoreCase("Dummy"))
        {
            try
            {
                returnList = new ArrayList<>(wrapper.getDataBaseAccess().readObject(identifier, localSnapshotId, clientId));
            }
            catch (final OutDatedDataException e)
            {
                kryo.writeObject(output, Constants.ABORT);
                kryo.writeObject(output, localSnapshotId);

                Log.getLogger().info("Transaction found conflict");
                Log.getLogger().info("OutdatedData Exception thrown: ", e);
                kryo.writeObject(output, new ArrayList<NodeStorage>());
                kryo.writeObject(output, new ArrayList<RelationshipStorage>());
                return output;
            }
        }
        else
        {
            returnList = new ArrayList<>();
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

        final ArrayList<NodeStorage> nodeStorage = new ArrayList<>();
        final ArrayList<RelationshipStorage> relationshipStorage = new ArrayList<>();
        for (final Object obj : returnList)
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
     * Create an empty ready response.
     *
     * @param message the message to write.
     * @param kryo    the kryo instance.
     * @return the generated output object.
     */
    Output makeEmptyReadResponse(final String message, final Kryo kryo)
    {
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, message);
        kryo.writeObject(output, new ArrayList<NodeStorage>());
        kryo.writeObject(output, new ArrayList<RelationshipStorage>());
        return output;
    }

    /**
     * Make an empty abort result.
     *
     * @return a byte array with it.
     */
    byte[] makeEmptyAbortResult()
    {
        final Output output = new Output(0, 128);
        final KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        final Kryo kryo = pool.borrow();
        kryo.writeObject(output, Constants.ABORT);
        final byte[] temp = output.getBuffer();
        output.close();
        pool.release(kryo);
        return temp;
    }

    /**
     * Handles the relationship read message and requests it to the database.
     *
     * @param input    get info from.
     * @param kryo     kryo object.
     * @param output   write info to.
     * @param clientId the id of the reading client.
     * @return output object to return to client.
     */
    Output handleRelationshipRead(final Input input, final Kryo kryo, final Output output, final int clientId)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        final RelationshipStorage identifier = kryo.readObject(input, RelationshipStorage.class);
        input.close();

        updateCounts(0, 1, 0, 0);

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            /**
             * TransactionStorage transaction = new TransactionStorage();
             * transaction.addReadSetRelationship(identifier);
             * localTransactionList.put(messageContext.getSender(), transaction);
             */

            localSnapshotId = getGlobalSnapshotId();
            if (!clients.containsKey(clientId) || clients.get(clientId) < localSnapshotId)
            {
                clients.put(clientId, localSnapshotId);
            }
        }
        ArrayList<Object> returnList = null;


        Log.getLogger().info("Get info from databaseAccess");
        try
        {
            returnList = new ArrayList<>((wrapper.getDataBaseAccess()).readObject(identifier, localSnapshotId, clientId));
        }
        catch (final OutDatedDataException e)
        {
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, localSnapshotId);

            Log.getLogger().info("Transaction found conflict");
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
        for (final Object obj : returnList)
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
     * Execute the commit on the replica.
     *  @param localWriteSet the write set to execute.
     * @param keyLoader     the key loader.
     * @param idClient      the id of the server.
     * @param consensusId the consensus ID.
     */
    void executeCommit(final List<IOperation> localWriteSet, final RSAKeyLoader keyLoader, final int idClient, final long clientSnapshot, final int consensusId)
    {
        //todo some way we have to detect what we executed already, to not try to catch up too much!
        if (!clients.containsKey(idClient) || clients.get(idClient) < clientSnapshot)
        {
            clients.put(idClient, clientSnapshot);
        }

        // First sign, then execute:
        final long currentSnapshot = ++globalSnapshotId;
        //Execute the transaction.
        for (final IOperation op : localWriteSet)
        {
            op.apply(wrapper.getDataBaseAccess(), globalSnapshotId, keyLoader, idClient);
            updateCounts(1, 0, 0, 0);
        }
        this.putIntoWriteSet(currentSnapshot, new ArrayList<>(localWriteSet));


        wrapper.setLastTransactionId(consensusId);
        updateCounts(0, 0, 1, 0);
    }

    /**
     * Put info in writeSet.
     *
     * @param currentSnapshot the current snapshot.
     * @param localWriteSet   data to add.
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
    public ServiceReplica getReplica()
    {
        return replica;
    }

    /**
     * Getter for the global snapshotId.
     *
     * @return the snapshot id.
     */
    public long getGlobalSnapshotId()
    {
        return globalSnapshotId;
    }

    /**
     * Get a copy of the global writeSet.
     *
     * @return a hashmap of all the operations with their snapshotId.
     */
    public ConcurrentSkipListMap<Long, List<IOperation>> getGlobalWriteSet()
    {
        return globalWriteSet;
    }

    /**
     * Get a copy of the global writeSet.
     *
     * @return a hashmap of all the operations with their snapshotId.
     */
    public Map<Long, List<IOperation>> getLatestWritesSet()
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
    public KryoFactory getFactory()
    {
        return factory;
    }

    /**
     * Set the globalSnapShotId to a certain value.
     *
     * @param globalSnapshotId the value to set.
     */
    public void setGlobalSnapshotId(final long globalSnapshotId)
    {
        this.globalSnapshotId = globalSnapshotId;
    }

    /**
     * Get the server instrumentation.
     * @return the instrumentation object.
     */
    public ServerInstrumentation getInstrumentation()
    {
        return instrumentation;
    }

    /**
     * Getter for the id.
     *
     * @return the id.
     */
    public int getId()
    {
        return id;
    }
}
