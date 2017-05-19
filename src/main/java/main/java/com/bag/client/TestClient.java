package main.java.com.bag.client;

import bftsmart.communication.client.ReplyReceiver;
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

import java.io.Closeable;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class handling the client.
 */
public class TestClient extends ServiceProxy implements BAGClient, ReplyReceiver, Closeable, AutoCloseable
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
     * Local timestamp of the current transaction.
     */
    private long localTimestamp = -1;

    /**
     * The id of the local server process the client is communicating with.
     */
    private final int serverProcess;

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
    private ServiceProxy globalProxy;

    /**
     * Create a threadsafe version of kryo.
     */
    private KryoFactory factory = () ->
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
        super(processId, localClusterId == -1 ? GLOBAL_CONFIG_LOCATION : String.format(LOCAL_CONFIG_LOCATION, localClusterId));

        if(localClusterId != -1)
        {
            globalProxy = new ServiceProxy(100 + getProcessId(), "global/config");
        }

        secureMode = true;
        this.serverProcess = serverId;
        this.localClusterId = localClusterId;
        initClient();
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
        if(identifier == null && value == null)
        {
            Log.getLogger().warn("Unsupported write operation");
            return;
        }

        //Must be a create request.
        if(identifier == null)
        {
            handleCreateRequest(value);
            return;
        }

        //Must be a delete request.
        if(value == null)
        {
            handleDeleteRequest(identifier);
            return;
        }

        handleUpdateRequest(identifier, value);
    }

    /**
     * Fills the updateSet in the case of an update request.
     * @param identifier the value to write to.
     * @param value what should be written.
     */
    private void handleUpdateRequest(Object identifier, Object value)
    {
        //todo edit create request if equal.
        if(identifier instanceof NodeStorage && value instanceof NodeStorage)
        {
            writeSet.add(new UpdateOperation<>((NodeStorage) identifier,(NodeStorage) value));
        }
        else if(identifier instanceof RelationshipStorage && value instanceof RelationshipStorage)
        {
            writeSet.add(new UpdateOperation<>((RelationshipStorage) identifier,(RelationshipStorage) value));
        }
        else
        {
            Log.getLogger().warn("Unsupported update operation can't update a node with a relationship or vice versa");
        }
    }

    /**
     * Fills the createSet in the case of a create request.
     * @param value object to fill in the createSet.
     */
    private void handleCreateRequest(Object value)
    {
        if(value instanceof NodeStorage)
        {
            writeSet.add(new CreateOperation<>((NodeStorage) value));
        }
        else if(value instanceof RelationshipStorage)
        {
            readsSetNode.add(((RelationshipStorage) value).getStartNode());
            readsSetNode.add(((RelationshipStorage) value).getEndNode());
            writeSet.add(new CreateOperation<>((RelationshipStorage) value));
        }
    }

    /**
     * Fills the deleteSet in the case of a delete requests and deletes the node also from the create set and updateSet.
     * @param identifier the object to delete.
     */
    private void handleDeleteRequest(Object identifier)
    {
        //todo we can delete creates here.
        if(identifier instanceof NodeStorage)
        {
            writeSet.add(new DeleteOperation<>((NodeStorage) identifier));
        }
        else if(identifier instanceof RelationshipStorage)
        {
            writeSet.add(new DeleteOperation<>((RelationshipStorage) identifier));
        }
    }

    /**
     * ReadRequests.(Directly read database) send the request to the db.
     * @param identifiers list of objects which should be read, may be NodeStorage or RelationshipStorage
     */
    @Override
    public void read(final Object...identifiers)
    {
        long timeStampToSend = firstRead ? -1 : localTimestamp;

        for(final Object identifier: identifiers)
        {
            if (identifier instanceof NodeStorage)
            {
                //this sends the message straight to server 0 not to the others.
                sendMessageToTargets(this.serialize(Constants.READ_MESSAGE, timeStampToSend, identifier), 0, new int[] {serverProcess}, TOMMessageType.UNORDERED_REQUEST);
            }
            else if (identifier instanceof RelationshipStorage)
            {
                sendMessageToTargets(this.serialize(Constants.RELATIONSHIP_READ_MESSAGE, timeStampToSend, identifier), 0, new int[] {serverProcess}, TOMMessageType.UNORDERED_REQUEST);
            }
            else
            {
                Log.getLogger().warn("Unsupported identifier: " + identifier.toString());
            }
        }
        firstRead = false;
    }

    /**
     * Receiving read requests replies here
     * @param reply the received message.
     */
    @Override
    public void replyReceived(final TOMMessage reply)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        Log.getLogger().info("reply");
        if(reply.getReqType() == TOMMessageType.UNORDERED_REQUEST)
        {
            final Input input = new Input(reply.getContent());
            switch(kryo.readObject(input, String.class))
            {
                case Constants.READ_MESSAGE:
                    processReadReturn(input);
                    break;
                case Constants.GET_PRIMARY:
                case Constants.COMMIT_RESPONSE:
                    handleReadOnlyCommit(reply.getContent(), kryo);
                    break;
                default:
                    Log.getLogger().info("Unexpected message type!");
                    break;
            }
            input.close();
        }
        else if(reply.getReqType() == TOMMessageType.REPLY || reply.getReqType() == TOMMessageType.ORDERED_REQUEST)
        {
            Log.getLogger().info("Commit return" + reply.getReqType().name());
            processCommitReturn(reply.getContent());
        }
        else
        {
            Log.getLogger().info("Receiving other type of request." + reply.getReqType().name());

        }
        pool.release(kryo);
        super.replyReceived(reply);
    }

    /**
     * Processes the return of a read request. Filling the readsets.
     * @param input the received bytes in an input..
     */
    private void processReadReturn(final Input input)
    {
        if(input == null)
        {
            Log.getLogger().warn("TimeOut, Didn't receive an answer from the server!");
            return;
        }

        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final String result = kryo.readObject(input, String.class);
        this.localTimestamp = kryo.readObject(input, Long.class);

        if(Constants.ABORT.equals(result))
        {
            input.close();
            pool.release(kryo);
            resetSets();
            readQueue.add(FINISHED_READING);
            return;
        }


        final List nodes = kryo.readObject(input, ArrayList.class);
        final List relationships = kryo.readObject(input, ArrayList.class);

        if(nodes != null && !nodes.isEmpty() && nodes.get(0) instanceof NodeStorage)
        {
            for (NodeStorage storage : (ArrayList<NodeStorage>) nodes)
            {
                NodeStorage tempStorage = new NodeStorage(storage.getId(), storage.getProperties());
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

        if(relationships != null && !relationships.isEmpty() && relationships.get(0) instanceof RelationshipStorage)
        {
            for (RelationshipStorage storage : (ArrayList<RelationshipStorage>)relationships)
            {
                RelationshipStorage tempStorage = new RelationshipStorage(storage.getId(), storage.getProperties(), storage.getStartNode(), storage.getEndNode());
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

        if(result == null)
        {
            Log.getLogger().warn("Server returned null, something went incredibly wrong there");
            return;
        }

        final Input input = new Input(result);
        final String type = kryo.readObject(input, String.class);

        if(!Constants.COMMIT_RESPONSE.equals(type))
        {
            Log.getLogger().warn("Incorrect response to commit message");
            input.close();
            return;
        }

        final String decision = kryo.readObject(input, String.class);
        localTimestamp = kryo.readObject(input, Long.class);

        if(Constants.COMMIT.equals(decision))
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
     * Handles read only commit responses.
     * @param answer the resulting string.
     * @param kryo the kryo object for deserialization and serialization.
     */
    private void handleReadOnlyCommit(final byte[] answer, final Kryo kryo)
    {
        final Input input = new Input(answer);
        Log.getLogger().info("Committed with snapshotId " + this.localTimestamp);
        final String messageType = kryo.readObject(input, String.class);

        if (!Constants.COMMIT_RESPONSE.equals(messageType))
        {
            Log.getLogger().warn("Incorrect response type to client from server!" + getProcessId());
            resetSets();
            firstRead = true;
            return;
        }

        final boolean commit = Constants.COMMIT.equals(kryo.readObject(input, String.class));
        if (commit)
        {
            localTimestamp = kryo.readObject(input, Long.class);
            resetSets();
            firstRead = true;
            Log.getLogger().info(String.format("Transaction with local transaction id: %d successfully committed", localTimestamp));
            return;
        }

        Log.getLogger().warn(String.format("Read-only Transaction with local transaction id: %d resend to the server as global", localTimestamp));

        if (localClusterId == -1)
        {
            Log.getLogger().info("Distribute commit with snapshotId: " + this.localTimestamp);
            invokeOrdered(serializeAll());
        }
        else
        {
            Log.getLogger().info("Commit with snapshotId directly to global cluster. TimestampId: " + this.localTimestamp);
            Log.getLogger().info("WriteSet: " + writeSet.size() + " readSetNode: " + readsSetNode.size() + " readSetRs: " + readsSetRelationship.size());
            processCommitReturn(globalProxy.invokeOrdered(serializeAll()));
        }
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
            Log.getLogger().info("Commit with snapshotId: " + this.localTimestamp);
            if(localClusterId == -1)
            {
                this.invokeUnordered(bytes);
            }
            else
            {
                //Normally its globaly.invokeUnordered
                Log.getLogger().warn("Send: " + bytes.length);
                this.invokeUnordered(bytes);
                Log.getLogger().warn("Sent: " + bytes.length);
            }
            return;
        }

        if (localClusterId == -1)
        {
            Log.getLogger().info("Distribute commit with snapshotId: " + this.localTimestamp);
            this.invokeOrdered(bytes);
        }
        else
        {
            Log.getLogger().info("Commit with snapshotId directly to global cluster. TimestampId: " + this.localTimestamp);
            Log.getLogger().info("WriteSet: " + writeSet.size() + " readSetNode: " + readsSetNode.size() + " readSetRs: " + readsSetRelationship.size());
            processCommitReturn(globalProxy.invokeOrdered(bytes));
        }
    }


    /**
     * Serializes the data and returns it in byte format.
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
     * @return the data in byte format.
     */
    private byte[] serialize(@NotNull String reason, long localTimestamp, final Object...args)
    {
        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Output output = new Output(0, 100024);
        kryo.writeObject(output, reason);
        kryo.writeObject(output, localTimestamp);

        for(final Object identifier: args)
        {
            if(identifier instanceof NodeStorage || identifier instanceof RelationshipStorage)
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
    private void resetSets()
    {
        readsSetNode = new ArrayList<>();
        readsSetRelationship = new ArrayList<>();
        writeSet = new ArrayList<>();
    }

    /**
     * Checks if the transaction has made any changes to the update sets.
     * @return true if not.
     */
    private boolean isReadOnly()
    {
        return writeSet.isEmpty();
    }

    /**
     * Get the primary of the cluster.
     * @param kryo the kryo instance.
     * @return the primary id.
     */
    private int getPrimary(final Kryo kryo)
    {
        byte[] response = invoke(serialize(Constants.GET_PRIMARY), TOMMessageType.UNORDERED_REQUEST);
        if(response == null)
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
}
