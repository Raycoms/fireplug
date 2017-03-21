package main.java.com.bag.server;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.TOMUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.SignatureStorage;
import main.java.com.bag.util.storage.TransactionStorage;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class LocalClusterSlave extends AbstractRecoverable
{
    /**
     * Name of the location of the global config.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global%d/config";
    /**
     * The place the local config file lays. This + the cluster id will contain the concrete cluster config location.
     */
    private static final String LOCAL_CONFIG_LOCATION = "local%d/config";

    /**
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    private final ServerWrapper wrapper;

    /**
     * Checks if this replica is primary in his cluster.
     */
    private boolean isPrimary = false;

    /**
     * The id of the local cluster.
     */
    private final int id;

    /**
     * The id of the primary of this slave.
     */
    private int primaryGlobalClusterId = -1;

    /**
     * The id of the primary of this slave in the local cluster.
     */
    private int primaryId = -1;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private final ServiceProxy proxy;

    //todo maybe detect local transaction problems in the future.
    /**
     * Contains all local transactions being executed on the server at the very moment.
     */
    private HashMap<Integer, TransactionStorage> localTransactionList = new HashMap<>();

    /**
     * Public constructor used to create a local cluster slave.
     * @param id its unique id in the local cluster.
     * @param wrapper its ordering wrapper.
     * @param localClusterId the id of the cluster (the id of the starting primary in the global cluster).
     */
    public LocalClusterSlave(final int id, @NotNull final ServerWrapper wrapper, final int localClusterId)
    {
        super(id, String.format(LOCAL_CONFIG_LOCATION, localClusterId), wrapper);
        this.id = id;
        this.wrapper = wrapper;
        this.proxy = new ServiceProxy(id , String.format(LOCAL_CONFIG_LOCATION, localClusterId));
    }

    /**
     * Set the local cluster instance to primary.
     * @param isPrimary true if so.
     */
    public void setPrimary(boolean isPrimary)
    {
        if(isPrimary)
        {
            primaryId = id;
        }
        this.isPrimary = isPrimary;
    }

    /**
     * Set the id of the primary global cluster.
     * @param primaryGlobalClusterId the id.
     */
    public void setPrimaryGlobalClusterId(final int primaryGlobalClusterId)
    {
        this.primaryGlobalClusterId = primaryGlobalClusterId;
    }

    /**
     * Check if the local cluster slave is a primary.
     * @return true if so.
     */
    public boolean isPrimary()
    {
        return isPrimary;
    }

    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
        return new byte[0][];
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message");
        KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);
        String reason = kryo.readObject(input, String.class);

        Output output = new Output(0, 1000240);
        switch (reason)
        {
            case Constants.READ_MESSAGE:
                output = handleNodeRead(input, messageContext, kryo, output);
                break;
            case Constants.RELATIONSHIP_READ_MESSAGE:
                output = handleRelationshipRead(input, messageContext, kryo, output);
                break;
            case Constants.GET_PRIMARY:
                output = handleGetPrimaryMessage(messageContext, output);
                break;
            case Constants.COMMIT:
                output = handleCommitMessage(input, messageContext, kryo, output);
                break;
            case Constants.PRIMARY_NOTICE:
                output = handlePrimaryNoticeMessage(input, output);
                break;
            case Constants.REGISTER_GLOBALLY_MESSAGE:
                output = handleRegisterGloballyMessage(input, output, messageContext);
                break;
            case Constants.UPDATE_SLAVE:
                 output = handleSlaveUpdateMessage(input, output, kryo);
                 break;
            case Constants.ASK_PRIMARY:
                notifyAllSlavesAboutNewPrimary();
                break;
            default:
                Log.getLogger().warn("Incorrect operation sent unordered to the server");
                output.close();
                input.close();
                return new byte[0];
        }

        //If primary changed ask new primary for his global cluster id.
        if(messageContext.getLeader() != primaryId)
        {
            primaryId = messageContext.getLeader();
            final Output localOutput = new Output(0, 512);
            localOutput.writeString(Constants.ASK_PRIMARY);
            proxy.sendMessageToTargets(output.getBuffer(), 0, new int[] {messageContext.getLeader()}, TOMMessageType.UNORDERED_REQUEST);
            localOutput.close();
        }

        byte[] returnValue = output.toBytes();

        Log.getLogger().info("Return it to client, size: " + returnValue.length);

        output.close();
        pool.release(kryo);

        return returnValue;
    }

    /**
     * Check if the primary is correct.
     * @param input the input.
     * @param output the presumed output.
     * @param messageContext the message context.
     * @return output obejct with decision.
     */
    private Output handleRegisterGloballyMessage(final Input input, final Output output, final MessageContext messageContext)
    {
        final int oldPrimary = input.readInt();
        final int newPrimary = input.readInt();

        output.writeString(Constants.REGISTER_GLOBALLY_REPLY);
        if(messageContext.getLeader() == newPrimary)
        {
            output.writeBoolean(true);
        }
        else
        {
            output.writeBoolean(false);
        }

        if(messageContext.getLeader() == oldPrimary)
        {
            Log.getLogger().warn("Slave: " + newPrimary + "tried to register as new primary.");
        }
        return output;
    }

    /**
     * Message to handle the primary notice.
     * @param input the input message.
     * @param output the output object to return.
     * @return an empty output message.
     */
    private Output handlePrimaryNoticeMessage(final Input input, final Output output)
    {
        this.primaryGlobalClusterId = input.readInt();
        input.close();
        return output;
    }

    /**
     * Handles a commit message on the client.
     * @param input the incoming message.
     * @param messageContext the context.
     * @param kryo the kryo object.
     * @param output the output object, future response.
     * @return the response in form of an Output object.
     */
    @NotNull
    private Output handleCommitMessage(final Input input, final MessageContext messageContext, final Kryo kryo, final Output output)
    {
        if (messageContext.getLeader() == id)
        {
            if(!isPrimary || wrapper.getGlobalCluster() == null)
            {
                isPrimary = true;
                if(!requestRegistering(proxy))
                {
                    isPrimary = false;
                    output.writeString(Constants.PENDING);
                    return output;
                }

                notifyAllSlavesAboutNewPrimary();
                wrapper.initNewGlobalClusterInstance();
            }
            return wrapper.getGlobalCluster().invokeGlobally(input);
        }

        if(wrapper.getGlobalCluster() != null)
        {
            wrapper.terminateGlobalCluster();
        }

        isPrimary = false;
        return output;
    }

    /**
     * Notify the slaves about their new primary.
     */
    private void notifyAllSlavesAboutNewPrimary()
    {
        final Output output = new Output(0, 1000240);
        output.writeString(Constants.PRIMARY_NOTICE);
        output.writeInt(wrapper.getGlobalServerId());
        primaryGlobalClusterId = wrapper.getGlobalServerId();

        proxy.invokeUnordered(output.getBuffer());

        output.close();
    }

    private boolean requestRegistering(final ServiceProxy proxy)
    {
        final ServiceProxy globalProxy = new ServiceProxy(id , "global" + id);

        final Output output = new Output(0, 1000240);
        output.writeString(Constants.REGISTER_GLOBALLY_MESSAGE);
        output.writeInt(wrapper.getLocalClusterSlaveId());
        output.writeInt(wrapper.getGlobalServerId());
        output.writeInt(primaryGlobalClusterId);

        byte[] result = proxy.invokeUnordered(output.getBuffer());

        final Input input = new Input(result);

        if(Constants.REGISTER_GLOBALLY_MESSAGE.equals(input.readString()) && input.readBoolean())
        {
            notifyAllSlavesAboutNewPrimary();
            input.close();
            globalProxy.close();
            output.close();
            return true;
        }

        input.close();
        globalProxy.close();
        output.close();
        return false;
    }

    /**
     * Handles a get primary message.
     * @param messageContext the message context.
     * @param output write info to.
     * @return sends the primary to the people.
     */
    private Output handleGetPrimaryMessage(final MessageContext messageContext, final Output output)
    {
        output.writeInt(messageContext.getLeader());
        return output;
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
    private Output handleNodeRead(Input input, MessageContext messageContext, Kryo kryo, Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        NodeStorage identifier = (NodeStorage) kryo.readClassAndObject(input);
        input.close();

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetNodes(identifier);
            localTransactionList.put(messageContext.getSender(), transaction);
            localSnapshotId = getGlobalSnapshotId();
        }
        ArrayList<Object> returnList = null;


        Log.getLogger().info("Get info from databaseAccess");

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
            kryo.writeClassAndObject(output, new ArrayList<NodeStorage>());
            kryo.writeClassAndObject(output, new ArrayList<RelationshipStorage>());
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

        kryo.writeClassAndObject(output, nodeStorage);
        kryo.writeClassAndObject(output, relationshipStorage);

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
    private Output handleRelationshipRead(final Input input, final MessageContext messageContext, final Kryo kryo, final Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        RelationshipStorage identifier = (RelationshipStorage) kryo.readClassAndObject(input);
        input.close();

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetRelationship(identifier);
            localTransactionList.put(messageContext.getSender(), transaction);
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
            kryo.writeClassAndObject(output, new ArrayList<NodeStorage>());
            kryo.writeClassAndObject(output, new ArrayList<RelationshipStorage>());
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
        kryo.writeClassAndObject(output, nodeStorage);
        kryo.writeClassAndObject(output, relationshipStorage);

        return output;
    }

    private Output handleSlaveUpdateMessage(final Input input, final Output output, final Kryo kryo)
    {
        final String decision = input.readString();
        final long snapShotId = input.readLong();
        final long lastKey = getGlobalSnapshotId();

        if(lastKey > snapShotId)
        {
            Log.getLogger().warn("Something went incredibly wrong. Transaction has been executed even with a missing one at local cluster: " + wrapper.getLocalClusterSlaveId());
            return output;
        }
        else if(lastKey == snapShotId)
        {
            Log.getLogger().info("Received already committed transaction.");
            output.writeBoolean(true);
            return output;
        }

        final SignatureStorage storage;

        try
        {
            storage = (SignatureStorage) kryo.readClassAndObject(input);
        }
        catch (ClassCastException exp)
        {
            Log.getLogger().warn("Unable to cast to SignatureStorage, something went wrong badly.", exp);
            return output;
        }

        Input messageInput = new Input(storage.getMessage());

        messageInput.readString();
        messageInput.readLong();
        final Object writeSet = kryo.readClassAndObject(messageInput);
        final ArrayList<Operation> localWriteSet;

        messageInput.close();

        try
        {
            localWriteSet = (ArrayList<Operation>) writeSet;
        }
        catch (ClassCastException e)
        {
            Log.getLogger().warn("Couldn't convert received signature message.", e);
            return output;
        }

        boolean signatureMatches;
        for(Map.Entry<Integer, byte[]> entry : storage.getSignatures().entrySet())
        {
            final RSAKeyLoader rsaLoader = new RSAKeyLoader(entry.getKey(), GLOBAL_CONFIG_LOCATION, false);
            try
            {
                signatureMatches = TOMUtil.verifySignature(rsaLoader.loadPublicKey(), storage.getMessage(), entry.getValue());
            }
            catch (Exception e)
            {
                signatureMatches = false;
                Log.getLogger().warn("Unable to load public key on server " + id + " of server: " + entry.getKey(), e);
            }

            if(!signatureMatches)
            {
                Log.getLogger().warn("Something went incredibly wrong. Transaction came without correct signatures from the primary at localCluster: " + wrapper.getLocalClusterSlaveId());
                return output;
            }
        }

        if(lastKey + 1 == snapShotId && Constants.COMMIT.equals(decision))
        {
            executeCommit(localWriteSet);
            output.writeBoolean(true);
            return output;
        }

        //TODO We might request the missing message here?
        Log.getLogger().warn("Something went wrong, missing a message");
        return output;
    }

    /**
     * Send this update to all other replicas.
     * @param storage the signatureStorage with message and signatures..
     */
    public void propagateUpdate(final SignatureStorage storage)
    {
        final KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Input input = new Input(storage.getMessage());
        final String decision = input.readString();
        final Long snapShotId = input.readLong();
        final Object writeSet = kryo.readClassAndObject(input);

        final Output output = new Output(10096);
        output.writeString(Constants.UPDATE_SLAVE);
        output.writeString(decision);
        output.writeLong(snapShotId);
        kryo.writeClassAndObject(output, storage);


        byte[] result = null;

        while(result == null || result.length == 0)
        {
            result = proxy.invokeUnordered(output.getBuffer());
        }

        if(Constants.COMMIT.equals(decision))
        {
            final ArrayList<Operation> localWriteSet;

            try
            {
                localWriteSet = (ArrayList<Operation>) writeSet;
            }
            catch (ClassCastException e)
            {
                Log.getLogger().warn("Couldn't convert received signature message.", e);
                return;
            }

            this.executeCommit(localWriteSet);
        }

        pool.release(kryo);
        output.close();
    }

    /**
     * Check if the oldPrimary switched to the newPrimary.
     * @param oldPrimary the old primary id.
     * @param newPrimary the new primary id.
     * @return true if correct.
     */
    public boolean askIfIsPrimary(final int oldPrimary, final int newPrimary)
    {
        final Output output = new Output(512);
        output.writeString(Constants.REGISTER_GLOBALLY_CHECK);
        output.writeInt(oldPrimary);
        output.writeInt(newPrimary);
        byte[] result = proxy.invokeOrdered(output.getBuffer());

        final Input input = new Input(result);
        if(Constants.REGISTER_GLOBALLY_MESSAGE.equals(input.readString()) && input.readBoolean())
        {
            input.close();
            output.close();
            return true;
        }
        input.close();
        output.close();
        return false;
    }

    @Override
    void readSpecificData(final Input input, final Kryo kryo)
    {
        isPrimary = false;
        primaryGlobalClusterId = input.readInt();
    }

    @Override
    void writeSpecificData(final Output output, final Kryo kryo)
    {
        output.writeInt(primaryGlobalClusterId);
    }
}
