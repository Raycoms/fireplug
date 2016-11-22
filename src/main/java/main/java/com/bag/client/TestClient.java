package main.java.com.bag.client;

import bftsmart.communication.client.ReplyReceiver;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Class handling the client.
 */
public class TestClient extends ServiceProxy implements ReplyReceiver, Closeable, AutoCloseable
{
    /**
     * Should the transaction run in secure mode?
     */
    private boolean secureMode = false;
    /**
     * Sets to log reads, updates, deletes and node creations.
     */
    private ArrayList<NodeStorage>         readsSetNode;
    private ArrayList<RelationshipStorage> readsSetRelationship;

    private ArrayList<Operation> writeSet;

    /**
     * Local timestamp of the current transaction.
     */
    private long localTimestamp = -1;

    /**
     *
     */
    private final int serverProcess;

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

    public TestClient(final int processId, final int serverId)
    {
        super(processId);
        secureMode = true;
        this.serverProcess = serverId;
        initClient();
    }

    public TestClient(final int processId, final String configHome)
    {
        super(processId, configHome);
        serverProcess = 0;
        initClient();
    }

    public TestClient(final int processId, final String configHome, final Comparator<byte[]> replyComparator, final Extractor replyExtractor)
    {
        super(processId, configHome, replyComparator, replyExtractor);
        serverProcess = 0;
        initClient();
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
     * write requests. (Only reach database on commit)
     */
    public void write(Object identifier, Object value)
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
            writeSet.add(new CreateOperation<>((RelationshipStorage) value));
        }
    }

    /**
     * Fills the deleteSet in the case of a delete requests and deletes the node also from the create set and updateSet.
     * @param identifier the object to delete.
     */
    private void handleDeleteRequest(Object identifier)
    {
        if(identifier instanceof NodeStorage)
        {
            writeSet.add(new CreateOperation<>((NodeStorage) identifier));
        }
        else if(identifier instanceof RelationshipStorage)
        {
            writeSet.add(new CreateOperation<>((RelationshipStorage) identifier));
        }
    }

    /**
     * ReadRequests.(Directly read database) send the request to the db.
     * @param identifiers list of objects which should be read, may be NodeStorage or RelationshipStorage
     */
    public void read(Object...identifiers)
    {
        for(Object identifier: identifiers)
        {
            if (identifier instanceof NodeStorage)
            {
                //this sends the message straight to server 0 not to the others.
                sendMessageToTargets(this.serialize(Constants.READ_MESSAGE, localTimestamp, identifier), 0, new int[] {serverProcess}, TOMMessageType.UNORDERED_REQUEST);
            }
            else if (identifier instanceof RelationshipStorage)
            {
                sendMessageToTargets(this.serialize(Constants.RELATIONSHIP_READ_MESSAGE, localTimestamp, identifier), 0, new int[] {serverProcess}, TOMMessageType.UNORDERED_REQUEST);
            }
            else
            {
                Log.getLogger().warn("Unsupported identifier: " + identifier.toString());
            }
        }
    }

    /**
     * Receiving read requests replies here
     * @param reply the received message.
     */
    @Override
    public void replyReceived(final TOMMessage reply)
    {
        Log.getLogger().info("reply");
        if(reply.getReqType() == TOMMessageType.UNORDERED_REQUEST)
        {
            processReadReturn(reply.getContent());
        }
        super.replyReceived(reply);
    }

    /**
     * Processes the return of a read request. Filling the readsets.
     * @param value the received bytes.
     */
    private void processReadReturn(byte[] value)
    {
        if(value == null)
        {
            Log.getLogger().warn("TimeOut, Didn't receive an answer from the server!");
            return;
        }

        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        Input input = new Input(value);
        localTimestamp = kryo.readObject(input, Long.class);

        Object nodes = kryo.readClassAndObject(input);
        Object relationships = kryo.readClassAndObject(input);

        if(nodes instanceof ArrayList && !((ArrayList) nodes).isEmpty() && ((ArrayList) nodes).get(0) instanceof NodeStorage)
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

        if(relationships instanceof ArrayList && !((ArrayList) relationships).isEmpty() && ((ArrayList) relationships).get(0) instanceof RelationshipStorage)
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

        input.close();
        pool.release(kryo);
    }
    
    /**
     * Commit reaches the server, if secure commit send to all, else only send to one
     */
    public void commit()
    {
        Log.getLogger().info("Starting commit");
        byte[] result;
        boolean readOnly = isReadOnly();

        byte[] bytes = serializeAll();
        if(readOnly && !secureMode)
        {
            Log.getLogger().info(String.format("Transaction with local transaction id: %d successfully commited", localTimestamp));
            resetSets();
            return;
        }
        else
        {
           result = invokeOrdered(bytes);
        }

        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        if(result == null)
        {
            Log.getLogger().warn("Server returned null, something went incredibly wrong there");
            return;
        }
        Input input = new Input(result);

        String type = input.readString();

        if(!Constants.COMMIT_RESPONSE.equals(type))
        {
            Log.getLogger().warn("Incorrect response to commit message");
            input.close();
            return;
        }

        String decision = input.readString();

        if(Constants.COMMIT.equals(decision))
        {
            Log.getLogger().info("Transaction succesfully committed");
        }
        else
        {
            Log.getLogger().info("Transaction commit denied - transaction being aborted");
        }

        resetSets();

        input.close();
        pool.release(kryo);
    }

    /**
     * Serializes the data and returns it in byte format.
     * @return the data in byte format.
     */
    private byte[] serialize(@NotNull String reason, long localTimestamp, Object...args)
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        Output output = new Output(0, 10024);

        kryo.writeObject(output, reason);
        kryo.writeObject(output, localTimestamp);
        for(Object identifier: args)
        {
            if(identifier instanceof NodeStorage || identifier instanceof RelationshipStorage)
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

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        Output output = new Output(0, 10024);

        kryo.writeObject(output, Constants.COMMIT_MESSAGE);
        //Write the timeStamp to the server
        kryo.writeObject(output, localTimestamp);

        //Write the readSet.
        kryo.writeClassAndObject(output, readsSetNode);
        kryo.writeClassAndObject(output, readsSetRelationship);

        //Write the writeSet.
        kryo.writeClassAndObject(output, writeSet);

        byte[] bytes = output.toBytes();
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

}
