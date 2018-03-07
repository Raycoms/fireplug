package main.java.com.bag.client;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class handling the client.
 */
public class TestClient implements BAGClient, ReplyListener
{
    /**
     * Should the transaction runNetty in secure mode?
     */
    private boolean secureMode = true;

    /**
     * The place the local config file is. This + the cluster id will contain the concrete cluster config location.
     */
    private static final String LOCAL_CONFIG_LOCATION = "local%d/config";

    /**
     * The place the global config files is.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global/config";

    /**
     * Sets to log reads, updates, deletes and node creations.
     */
    private ArrayList<NodeStorage>         readsSetNode;
    private ArrayList<RelationshipStorage> readsSetRelationship;

    private ArrayList<IOperation> writeSet;

    /**
     * Defines if the client is currently committing.
     */
    private boolean isCommitting = false;

    /**
     * Local timestamp of the current transaction.
     */
    private long localTimestamp = -1;

    /**
     * The id of the local server process the client is communicating with.
     */
    private int serverProcess;

    /**
     * Lock object to let the thread wait for a read return.
     */
    private BlockingQueue<Object> readQueue = new LinkedBlockingQueue<>();

    /**
     * The last object in read queue.
     */
    public static final Object FINISHED_READING = new Object();

    /**
     * Id of the local cluster.
     */
    private final int localClusterId;

    /**
     * Checks if its the first read of the client.
     */
    private boolean firstRead = true;

    /**
     * The proxy to use during communication with the globalCluster.
     */
    private AsynchServiceProxy globalProxy;

    private final Random random = new Random();

    /**
     * The proxy to use during communication with the globalCluster.
     */
    private AsynchServiceProxy localProxy;

    /**
     * The reply listener for aynch requests.
     */
    private ReplyListener bagReplyListener;

    private static final Comparator<byte[]> comparator = (o1, o2) ->
    {
        if (Arrays.equals(o1, o2))
        {
            return 0;
        }
        
        final Kryo kryo = new Kryo();
        try (final Input input1 = new Input(o1); final Input input2 = new Input(o2))
        {
            if(o1.length == 0 || o2.length == 0)
            {
                Log.getLogger().error("WOW, 1 of the messages has 0 length");
                return 0;
            }

            final String messageType1 = kryo.readObject(input1, String.class);
            final String messageType2 = kryo.readObject(input2, String.class);


            if (!messageType1.equals(messageType2))
            {
                Log.getLogger().error("Message types differ: " + messageType1 + " : " + messageType2);
                return -1;
            }

            if (messageType1.equals(Constants.COMMIT_RESPONSE))
            {
                final String commit1 = kryo.readObject(input1, String.class);
                final String commit2 = kryo.readObject(input2, String.class);

                if (!commit1.equals(commit2))
                {
                    Log.getLogger().error("Commit responses differ: " + commit1 + " : " + commit2);
                    return -1;
                }
            }
            else
            {
                Log.getLogger().error("Something went wrong, those messages are no commit responses: " + messageType1 + " " + messageType2);
            }
        }
        catch (final Exception e)
        {
            Log.getLogger().error("Something went wrong deserializing:" + e.toString());
            return -1;
        }

        return 0;
    };

    /**
     * Create a threadsafe version of kryo.
     */
    public KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        return kryo;
    };

    public TestClient(final int processId, final int serverId, final int localClusterId)
    {
        super();
        localProxy = new AsynchServiceProxy(processId, localClusterId == -1 ? GLOBAL_CONFIG_LOCATION : String.format(LOCAL_CONFIG_LOCATION, localClusterId), comparator, null);

        if (localClusterId != -1)
        {
            globalProxy = new AsynchServiceProxy(100 + processId, "global/config", comparator, null);
        }

        secureMode = true;
        this.serverProcess = serverId;
        this.localClusterId = localClusterId;
        initClient();
        bagReplyListener = new BAGReplyListener(this);

        Log.getLogger().warn("Starting client " + processId);
    }

    /**
     * Initiates the client maps and registers necessary operations.
     */
    private void initClient()
    {
        readsSetNode = new ArrayList<>();
        readsSetRelationship = new ArrayList<>();
        writeSet = new ArrayList<>();
    }

    /**
     * Get the blocking queue.
     *
     * @return the queue.
     */
    @Override
    public BlockingQueue<Object> getReadQueue()
    {
        return readQueue;
    }

    /**
     * write requests. (Only reach database on commit)
     */
    @Override
    public void write(final Object identifier, final Object value)
    {
        if (identifier == null && value == null)
        {
            Log.getLogger().warn("Unsupported write operation");
            return;
        }

        //Must be a create request.
        if (identifier == null)
        {
            handleCreateRequest(value);
            return;
        }

        //Must be a delete request.
        if (value == null)
        {
            handleDeleteRequest(identifier);
            return;
        }

        handleUpdateRequest(identifier, value);
    }

    /**
     * Fills the updateSet in the case of an update request.
     *
     * @param identifier the value to write to.
     * @param value      what should be written.
     */
    private void handleUpdateRequest(Object identifier, Object value)
    {
        if (identifier instanceof NodeStorage && value instanceof NodeStorage)
        {
            writeSet.add(new UpdateOperation<>((NodeStorage) identifier, (NodeStorage) value));
        }
        else if (identifier instanceof RelationshipStorage && value instanceof RelationshipStorage)
        {
            writeSet.add(new UpdateOperation<>((RelationshipStorage) identifier, (RelationshipStorage) value));
        }
        else
        {
            Log.getLogger().warn("Unsupported update operation can't update a node with a relationship or vice versa");
        }
    }

    /**
     * Fills the createSet in the case of a create request.
     *
     * @param value object to fill in the createSet.
     */
    private void handleCreateRequest(Object value)
    {
        if (value instanceof NodeStorage)
        {
            writeSet.add(new CreateOperation<>((NodeStorage) value));
        }
        else if (value instanceof RelationshipStorage)
        {
            readsSetNode.add(((RelationshipStorage) value).getStartNode());
            readsSetNode.add(((RelationshipStorage) value).getEndNode());
            writeSet.add(new CreateOperation<>((RelationshipStorage) value));
        }
    }

    /**
     * Fills the deleteSet in the case of a delete requests and deletes the node also from the create set and updateSet.
     *
     * @param identifier the object to delete.
     */
    private void handleDeleteRequest(Object identifier)
    {
        if (identifier instanceof NodeStorage)
        {
            writeSet.add(new DeleteOperation<>((NodeStorage) identifier));
        }
        else if (identifier instanceof RelationshipStorage)
        {
            writeSet.add(new DeleteOperation<>((RelationshipStorage) identifier));
        }
    }

    /**
     * ReadRequests.(Directly read database) send the request to the db.
     *
     * @param identifiers list of objects which should be read, may be NodeStorage or RelationshipStorage
     */
    @Override
    public void read(final Object... identifiers)
    {
        long timeStampToSend = firstRead ? -1 : localTimestamp;

        for (final Object identifier : identifiers)
        {
            if (identifier instanceof NodeStorage)
            {
                //this sends the message straight to server 0 not to the others.
                localProxy.invokeAsynchRequest(this.serialize(Constants.READ_MESSAGE, timeStampToSend, identifier),  new int[] {serverProcess}, this, TOMMessageType.UNORDERED_REQUEST);
            }
            else if (identifier instanceof RelationshipStorage)
            {
                localProxy.invokeAsynchRequest(this.serialize(Constants.RELATIONSHIP_READ_MESSAGE, timeStampToSend, identifier),
                        new int[] {serverProcess},
                        this,
                        TOMMessageType.UNORDERED_REQUEST);
            }
            else
            {
                Log.getLogger().warn("Unsupported identifier: " + identifier.toString());
            }
        }
        firstRead = false;
    }

    @Override
    public void replyReceived(final RequestContext requestContext, final TOMMessage tomMessage)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        Log.getLogger().info("reply");
        if (tomMessage.getReqType() == TOMMessageType.UNORDERED_REQUEST)
        {
            final Input input = new Input(tomMessage.getContent());
            switch (kryo.readObject(input, String.class))
            {
                case Constants.READ_MESSAGE:
                    processReadReturn(input);
                    break;
                case Constants.GET_PRIMARY:
                case Constants.COMMIT_RESPONSE:
                    pool.release(kryo);
                    return;
                default:
                    Log.getLogger().info("Unexpected message type!");
                    break;
            }
            input.close();
        }
        else if (tomMessage.getReqType() == TOMMessageType.REPLY || tomMessage.getReqType() == TOMMessageType.ORDERED_REQUEST)
        {
            pool.release(kryo);
            Log.getLogger().info("Commit return" + tomMessage.getReqType().name());
            return;
        }
        else
        {
            Log.getLogger().info("Receiving other type of request." + tomMessage.getReqType().name());
        }
        pool.release(kryo);
    }

    /**
     * Processes the return of a read request. Filling the readsets.
     *
     * @param input the received bytes in an input..
     */
    private void processReadReturn(final Input input)
    {
        if (input == null)
        {
            Log.getLogger().warn("TimeOut, Didn't receive an answer from the server!");
            return;
        }

        Log.getLogger().info("Process read return!");

        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final String result = kryo.readObject(input, String.class);
        this.localTimestamp = kryo.readObject(input, Long.class);

        if (Constants.ABORT.equals(result))
        {
            input.close();
            pool.release(kryo);
            resetSets();
            readQueue.add(FINISHED_READING);
            return;
        }

        final List nodes = kryo.readObject(input, ArrayList.class);
        final List relationships = kryo.readObject(input, ArrayList.class);

        if (nodes != null && !nodes.isEmpty() && nodes.get(0) instanceof NodeStorage)
        {
            for (final NodeStorage storage : (ArrayList<NodeStorage>) nodes)
            {
                final NodeStorage tempStorage = new NodeStorage(storage.getId(), storage.getProperties());
                try
                {
                    tempStorage.addProperty("hash", HashCreator.sha1FromNode(storage));
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.getLogger().warn("Couldn't add hash for node", e);
                }
                readsSetNode.add(tempStorage);
            }
        }

        if (relationships != null && !relationships.isEmpty() && relationships.get(0) instanceof RelationshipStorage)
        {
            for (final RelationshipStorage storage : (ArrayList<RelationshipStorage>) relationships)
            {
                final RelationshipStorage tempStorage = new RelationshipStorage(storage.getId(), storage.getProperties(), storage.getStartNode(), storage.getEndNode());
                try
                {
                    tempStorage.addProperty("hash", HashCreator.sha1FromRelationship(storage));
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.getLogger().warn("Couldn't add hash for relationship", e);
                }
                readsSetRelationship.add(tempStorage);
            }
        }

        readQueue.add(FINISHED_READING);
        input.close();
        pool.release(kryo);
    }

    private void processCommitReturn(final byte[] result)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        if (result == null)
        {
            Log.getLogger().warn("Server returned null, something went incredibly wrong there");
            resetSets();
            return;
        }

        final Input input = new Input(result);
        final String type = kryo.readObject(input, String.class);

        if (!Constants.COMMIT_RESPONSE.equals(type))
        {
            Log.getLogger().warn("Incorrect response to commit message");
            input.close();
            resetSets();
            return;
        }

        final String decision = kryo.readObject(input, String.class);
        localTimestamp = kryo.readObject(input, Long.class);

        Log.getLogger().info("Processing commit return: " + localTimestamp);

        if (Constants.COMMIT.equals(decision))
        {
            Log.getLogger().info("Transaction succesfully committed");
        }
        else
        {
            Log.getLogger().info("Transaction commit denied - transaction being aborted");
        }

        Log.getLogger().info("Reset after commit");
        resetSets();

        input.close();
        pool.release(kryo);
    }

    /**
     * Commit reaches the server, if secure commit send to all, else only send to one
     */
    @Override
    public void commit()
    {
        firstRead = true;
        final boolean readOnly = isReadOnly();
        Log.getLogger().info("Starting commit");

        if (readOnly && !secureMode)
        {
            Log.getLogger().warn(String.format("Read only unsecure Transaction with local transaction id: %d successfully committed", localTimestamp));
            firstRead = true;
            resetSets();
            return;
        }

        Log.getLogger().info("Starting commit process for: " + this.localTimestamp);
        final byte[] bytes = serializeAll();

        if (readOnly)
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            Log.getLogger().info(localProxy.getProcessId() + " Read-only Commit with snapshotId: " + this.localTimestamp);

            final byte[] answer;
            if (localClusterId == -1)
            {
                answer = localProxy.invokeUnordered(bytes);
            }
            else
            {
                //TODO test with 2 and three (three is odd one out, only needs one random call)
                //Do it in optimistic mode in local cluster (if >= 4 replicas)
                if(localProxy.getViewManager().getCurrentViewProcesses().length >= 4 && false)
                {
                    final int[] viewProcesses = localProxy.getViewManager().getCurrentViewProcesses();
                    final int rand1 = random.nextInt(viewProcesses.length);
                    int rand2 = random.nextInt(viewProcesses.length);

                    while (rand1 != rand2)
                    {
                        rand2 = random.nextInt(viewProcesses.length);
                    }

                    localProxy.invokeAsynchRequest(bytes, new int[]{rand1, rand2}, bagReplyListener, TOMMessageType.UNORDERED_REQUEST);
                    return;
                    Log.getLogger().info("To Local proxy:");
                    answer = localProxy.invokeUnordered(bytes);
                }
                else
                {
                    final int[] viewProcesses = globalProxy.getViewManager().getCurrentViewProcesses();
                    final int rand1 = random.nextInt(viewProcesses.length);
                    int rand2 = random.nextInt(viewProcesses.length);

                    while (rand1 != rand2)
                    {
                        rand2 = random.nextInt(viewProcesses.length);
                    }

                    globalProxy.invokeAsynchRequest(bytes, new int[]{rand1, rand2}, bagReplyListener, TOMMessageType.UNORDERED_REQUEST);
                    return;
                    //answer = globalProxy.invokeUnordered(bytes);
                }
            }

            Log.getLogger().info(localProxy.getProcessId() + "Committed with snapshotId " + this.localTimestamp);

            final Input input = new Input(answer);
            final String messageType = kryo.readObject(input, String.class);

            if (!Constants.COMMIT_RESPONSE.equals(messageType))
            {
                Log.getLogger().warn("Incorrect response type to client from server!" + localProxy.getProcessId());
                resetSets();
                firstRead = true;
                pool.release(kryo);
                return;
            }

            final boolean commit = Constants.COMMIT.equals(kryo.readObject(input, String.class));
            if (commit)
            {
                localTimestamp = kryo.readObject(input, Long.class);
                resetSets();
                firstRead = true;
                Log.getLogger().info(String.format("Transaction with local transaction id: %d successfully committed", localTimestamp));
                pool.release(kryo);
                return;
            }

            pool.release(kryo);
            resetSets();
            return;
        }

        if (localClusterId == -1)
        {
            Log.getLogger().info("Distribute commit with snapshotId: " + this.localTimestamp);
            localProxy.invokeOrdered(bytes);
        }
        else
        {
            Log.getLogger().info("Commit with snapshotId directly to global cluster. TimestampId: " + this.localTimestamp);
            Log.getLogger().info("WriteSet: " + writeSet.size() + " readSetNode: " + readsSetNode.size() + " readSetRs: " + readsSetRelationship.size());
            Log.getLogger().info(localProxy.getProcessId() + " Write (Ordered) Commit with snapshotId: " + this.localTimestamp);

            processCommitReturn(globalProxy.invokeOrdered(bytes));
        }
    }

    /**
     * Serializes the data and returns it in byte format.
     *
     * @return the data in byte format.
     */
    private byte[] serialize(@NotNull String request)
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        Output output = new Output(0, 256);
        kryo.writeObject(output, request);

        byte[] bytes = output.getBuffer();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Serializes the data and returns it in byte format.
     *
     * @return the data in byte format.
     */
    private byte[] serialize(@NotNull String reason, long localTimestamp, final Object... args)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Output output = new Output(0, 100024);
        kryo.writeObject(output, reason);
        kryo.writeObject(output, localTimestamp);

        for (final Object identifier : args)
        {
            if (identifier instanceof NodeStorage || identifier instanceof RelationshipStorage)
            {
                kryo.writeObject(output, identifier);
            }
        }

        byte[] bytes = output.getBuffer();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Serializes all sets and returns it in byte format.
     *
     * @return the data in byte format.
     */
    private byte[] serializeAll()
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Output output = new Output(0, 400024);

        kryo.writeObject(output, Constants.COMMIT_MESSAGE);
        //Write the timeStamp to the server
        kryo.writeObject(output, localTimestamp);

        //Write the readSet.
        kryo.writeObject(output, readsSetNode);
        kryo.writeObject(output, readsSetRelationship);

        //Write the writeSet.
        kryo.writeObject(output, writeSet);

        byte[] bytes = output.getBuffer();
        output.close();
        pool.release(kryo);
        return bytes;
    }

    /**
     * Resets all the read and write sets.
     */
    public void resetSets()
    {
        readsSetNode = new ArrayList<>();
        readsSetRelationship = new ArrayList<>();
        writeSet = new ArrayList<>();
        isCommitting = false;
        bagReplyListener.reset();
        //serverProcess = random.nextInt(4);
    }

    /**
     * Checks if the transaction has made any changes to the update sets.
     *
     * @return true if not.
     */
    private boolean isReadOnly()
    {
        return writeSet.isEmpty();
    }

    @Override
    public boolean isCommitting()
    {
        return isCommitting;
    }

    /**
     * Get the primary of the cluster.
     *
     * @param kryo the kryo instance.
     * @return the primary id.
     */
    private int getPrimary(final Kryo kryo)
    {
        byte[] response = localProxy.invoke(serialize(Constants.GET_PRIMARY), TOMMessageType.UNORDERED_REQUEST);
        if (response == null)
        {
            Log.getLogger().warn("Server returned null, something went incredibly wrong there");
            return -1;
        }

        final Input input = new Input(response);
        kryo.readObject(input, String.class);
        final int primaryId = kryo.readObject(input, Integer.class);

        Log.getLogger().info("Received id: " + primaryId);

        input.close();

        return primaryId;
    }

    @Override
    public void reset()
    {
        localProxy.close();
        globalProxy.close();
    }

    /**
     * Set the first read of the first read param. (Resetting it for next commit).
     * @param firstRead the var to set.
     */
    public void setFirstRead(final boolean firstRead)
    {
        this.firstRead = firstRead;
    }

    /**
     * Getter for the local timeStamp.
     * @return the long representing it.
     */
    public long getLocalTimestamp()
    {
        return localTimestamp;
    }

    /**
     * Setter for the local timeStamp.
     * @param localTimestamp the value to set.
     */
    public void setLocalTimestamp(final long localTimestamp)
    {
        this.localTimestamp = localTimestamp;
    }
}
