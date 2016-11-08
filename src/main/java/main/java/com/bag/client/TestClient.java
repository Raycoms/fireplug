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
import main.java.com.bag.util.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;

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
    private ArrayList<NodeStorage> readsSetNode;
    private HashMap<NodeStorage, NodeStorage> updateSetNode;
    private ArrayList<NodeStorage>            deleteSetNode;
    private ArrayList<NodeStorage>            createSetNode;

    /**
     * Sets to log reads, updates, deletes and relationship creations.
     */
    private ArrayList<RelationshipStorage> readsSetRelationship;
    private HashMap<RelationshipStorage, RelationshipStorage> updateSetRelationship;
    private ArrayList<RelationshipStorage>                    deleteSetRelationship;
    private ArrayList<RelationshipStorage>                    createSetRelationship;

    /**
     * Local timestamp of the current transaction.
     */
    private long localTimestamp = 0;

    /**
     * Create a threadsafe version of kryo.
     */
    private KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        return kryo;
    };

    public TestClient(final int processId)
    {
        super(processId);
        secureMode = true;
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
        readsSetNode = new ArrayList<>();
        updateSetNode = new HashMap<>();
        deleteSetNode = new ArrayList<>();
        createSetNode = new ArrayList<>();

        readsSetRelationship = new ArrayList<>();
        updateSetRelationship = new HashMap<>();
        deleteSetRelationship = new ArrayList<>();
        createSetRelationship = new ArrayList<>();
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
     * Since we will execute writes after creates and before deletes. We don't have to check the other sets.
     * @param identifier the value to write to.
     * @param value what should be written.
     */
    private void handleUpdateRequest(Object identifier, Object value)
    {
        if(identifier instanceof NodeStorage && value instanceof NodeStorage)
        {
            updateSetNode.put((NodeStorage) identifier, (NodeStorage) value);

            //todo if we change a node, we have to change the relationships with the same node as well.
            //todo run through create , update, deleteSet of relationship and check the node.
        }
        else if(identifier instanceof RelationshipStorage && value instanceof RelationshipStorage)
        {
            updateSetRelationship.put((RelationshipStorage) identifier, (RelationshipStorage) value);
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
            createSetNode.add((NodeStorage) value);
        }
        else if(value instanceof RelationshipStorage)
        {
            createSetRelationship.add((RelationshipStorage) value);
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
            updateSetNode.remove(identifier);
            createSetNode.remove(identifier);

            deleteSetNode.add((NodeStorage) identifier);

            new ArrayList<>(createSetRelationship).stream().filter(storage -> storage.getStartNode().equals(identifier)).forEach(storage -> createSetRelationship.remove(storage));
            new ArrayList<>(deleteSetRelationship).stream().filter(storage -> storage.getStartNode().equals(identifier)).forEach(storage -> deleteSetRelationship.remove(storage));
            new ArrayList<>(updateSetRelationship.keySet()).stream().filter(storage -> storage.getStartNode().equals(identifier)).forEach(storage -> updateSetRelationship.keySet().remove(storage));
        }
        else if(identifier instanceof RelationshipStorage)
        {
            updateSetRelationship.remove(identifier);
            createSetRelationship.remove(identifier);

            deleteSetRelationship.add((RelationshipStorage) identifier);
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
                sendMessageToTargets(this.serialize(Constants.READ_MESSAGE, localTimestamp, identifier), 0, new int[] {0}, TOMMessageType.UNORDERED_REQUEST);
            }
            else if (identifier instanceof RelationshipStorage)
            {
                sendMessageToTargets(this.serialize(Constants.RELATIONSHIP_READ_MESSAGE, localTimestamp, identifier), 0, new int[] {0}, TOMMessageType.UNORDERED_REQUEST);
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
     * Resets all the read and write sets.
     */
    private void resetSets()
    {
        readsSetNode = new ArrayList<>();
        updateSetNode = new HashMap<>();
        deleteSetNode = new ArrayList<>();
        createSetNode = new ArrayList<>();

        readsSetRelationship = new ArrayList<>();
        updateSetRelationship = new HashMap<>();
        deleteSetRelationship = new ArrayList<>();
        createSetRelationship = new ArrayList<>();
    }

    /**
     * Checks if the transaction has made any changes to the update sets.
     * @return true if not.
     */
    private boolean isReadOnly()
    {
        return hadNoNodeWrites() && hadNoRelationshipWrites();
    }

    /**
     * Checks if there were writes in the node-sets.
     * @return true if not.
     */
    private boolean hadNoNodeWrites()
    {
        return updateSetNode.isEmpty() && deleteSetNode.isEmpty() && createSetNode.isEmpty();
    }

    /**
     * Checks if there were writes in the relationship-sets.
     * @return true if not.
     */
    private boolean hadNoRelationshipWrites()
    {
        return updateSetRelationship.isEmpty() && deleteSetRelationship.isEmpty() && createSetRelationship.isEmpty();
    }

}
