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
import main.java.com.bag.operations.Operation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.SignatureStorage;
import org.jetbrains.annotations.NotNull;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Class handling server communication in the global cluster.
 */
public class GlobalClusterSlave extends AbstractRecoverable
{
    /**
     * Name of the location of the global config.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global/config";

    /**
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    private final ServerWrapper wrapper;

    /**
     * The id of the local cluster.
     */
    private final int id;

    /**
     * Map which holds the signatureStorages for the consistency.
     */
    private final Map<Long, SignatureStorage> signatureStorageMap = new TreeMap<>();

    /**
     * Lock used to synchronize access to signatureStorageMap.
     */
    private final Object lock = new Object();

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private final ServiceProxy proxy;

    public GlobalClusterSlave(final int id, @NotNull final ServerWrapper wrapper)
    {
        super(id, GLOBAL_CONFIG_LOCATION, wrapper);
        this.id = id;
        this.wrapper = wrapper;
        this.proxy = new ServiceProxy(1000 + id, GLOBAL_CONFIG_LOCATION);
        Log.getLogger().info("Turned on global cluster with id:" + id);
    }

    private byte[] makeEmptyAbortResult()
    {
        final Output output = new Output(0, 128);
        final KryoPool pool = new KryoPool.Builder(super.getFactory()).softReferences().build();
        final Kryo kryo = pool.borrow();
        kryo.writeObject(output, Constants.ABORT);
        byte[] temp = output.getBuffer();
        output.close();
        pool.release(kryo);
        return temp;
    }

    //Every byte array is one request.
    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
        byte[][] allResults = new byte[bytes.length][];
        for (int i = 0; i < bytes.length; ++i)
        {
            if (messageContexts != null && messageContexts[i] != null)
            {
                KryoPool pool = new KryoPool.Builder(super.getFactory()).softReferences().build();
                Kryo kryo = pool.borrow();
                Input input = new Input(bytes[i]);

                String type = kryo.readObject(input, String.class);

                if (Constants.COMMIT_MESSAGE.equals(type))
                {
                    final Long timeStamp = kryo.readObject(input, Long.class);
                    byte[] result = executeCommit(kryo, input, timeStamp);
                    pool.release(kryo);
                    allResults[i] = result;
                }
                else
                {
                    Log.getLogger().error("Return empty bytes for message type: " + type);
                    allResults[i] = makeEmptyAbortResult();
                    updateCounts(0, 0, 0, 1);
                }
            }
            else
            {
                Log.getLogger().error("Received message with empty context!");
                allResults[i] = makeEmptyAbortResult();
                updateCounts(0, 0, 0, 1);
            }
        }

        return allResults;
    }

    @Override
    void readSpecificData(final Input input, final Kryo kryo)
    {
        final int length = kryo.readObject(input, Integer.class);
        for (int i = 0; i < length; i++)
        {
            try
            {
                signatureStorageMap.put(kryo.readObject(input, Long.class), kryo.readObject(input, SignatureStorage.class));
            }
            catch (ClassCastException ex)
            {
                Log.getLogger().warn("Unable to restore signatureStoreMap entry: " + i + " at server: " + id, ex);
            }
        }
    }

    @Override
    void writeSpecificData(final Output output, final Kryo kryo)
    {
        if (signatureStorageMap != null)
        {
            kryo.writeObject(output, signatureStorageMap.size());
            for (Map.Entry<Long, SignatureStorage> entrySet : signatureStorageMap.entrySet())
            {
                kryo.writeObject(output, entrySet.getKey());
                kryo.writeObject(output, entrySet.getValue());
            }
        }
    }

    /**
     * Check for conflicts and unpack things for conflict handle check.
     *
     * @param kryo  the kryo instance.
     * @param input the input.
     * @return the response.
     */
    private byte[] executeCommit(final Kryo kryo, final Input input, final long timeStamp)
    {
        //Read the inputStream.
        final List readsSetNodeX = kryo.readObject(input, ArrayList.class);
        final List readsSetRelationshipX = kryo.readObject(input, ArrayList.class);
        final List writeSetX = kryo.readObject(input, ArrayList.class);

        //Create placeHolders.
        ArrayList<NodeStorage> readSetNode;
        ArrayList<RelationshipStorage> readsSetRelationship;
        ArrayList<Operation> localWriteSet;

        input.close();
        Output output = new Output(128);
        kryo.writeObject(output, Constants.COMMIT_RESPONSE);

        try
        {
            readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
            readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            localWriteSet = (ArrayList<Operation>) writeSetX;
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

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(), localWriteSet, readSetNode, readsSetRelationship, timeStamp, wrapper.getDataBaseAccess()))
        {
            updateCounts(0, 0, 0, 1);

            Log.getLogger()
                    .info("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: " + localWriteSet.size()
                            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            byte[] returnBytes = output.getBuffer();
            output.close();

            if (wrapper.getLocalCLuster() != null)
            {
                signCommitWithDecisionAndDistribute(localWriteSet, Constants.ABORT, getGlobalSnapshotId(), kryo);
            }
            return returnBytes;
        }

        if (!localWriteSet.isEmpty())
        {
            super.executeCommit(localWriteSet);
            if (wrapper.getLocalCLuster() != null)
            {
                signCommitWithDecisionAndDistribute(localWriteSet, Constants.COMMIT, getGlobalSnapshotId(), kryo);
            }
        }
        else
        {
            updateCounts(0, 0, 1, 0);
        }

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
    }

    private void signCommitWithDecisionAndDistribute(final List<Operation> localWriteSet, final String decision, final long snapShotId, final Kryo kryo)
    {
        Log.getLogger().info("Sending signed commit to the other global replicas");
        final RSAKeyLoader rsaLoader = new RSAKeyLoader(1000 + this.id, GLOBAL_CONFIG_LOCATION, false);

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        final Output output = new Output(0, 100240);

        kryo.writeObject(output, Constants.SIGNATURE_MESSAGE);
        kryo.writeObject(output, decision);
        kryo.writeObject(output, snapShotId);
        kryo.writeObject(output, localWriteSet);

        final byte[] message = output.toBytes();
        final byte[] signature;
        try
        {
            signature = TOMUtil.signMessage(rsaLoader.loadPrivateKey(), message);
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Unable to sign message at server " + id, e);
            return;
        }

        final SignatureStorage signatureStorage;

        synchronized (lock)
        {
            if (signatureStorageMap.containsKey(getGlobalSnapshotId()))
            {
                signatureStorage = signatureStorageMap.get(getGlobalSnapshotId());
                if (signatureStorage.getMessage().length != output.toBytes().length)
                {
                    Log.getLogger().error("Message in signatureStorage: " + signatureStorage.getMessage().length + " message of committing server: " + message.length);
                }
            }
            else
            {
                Log.getLogger().info("Size of message stored is: " + message.length);
                signatureStorage = new SignatureStorage(super.getReplica().getReplicaContext().getStaticConfiguration().getN() - 1, message, decision);
                signatureStorageMap.put(snapShotId, signatureStorage);
            }

            signatureStorage.setProcessed();
            Log.getLogger().warn("Set processed by global cluster: " + snapShotId);
            signatureStorage.addSignatures(id + 1000, signature);

            if (signatureStorage.hasEnough())
            {
                Log.getLogger().warn("Sending update to slave signed by all members: " + snapShotId);
                updateSlave(signatureStorage);
                signatureStorageMap.remove(snapShotId);
                return;
            }
        }

        kryo.writeObject(output, message.length);
        kryo.writeObject(output, signature.length);
        output.writeBytes(signature);
        proxy.sendMessageToTargets(output.getBuffer(), 0, proxy.getViewManager().getCurrentViewProcesses(), TOMMessageType.UNORDERED_REQUEST);
        output.close();
    }

    private Output makeEmptyReadResponse(String message, Kryo kryo)
    {
        final Output output = new Output(0, 10240);
        kryo.writeObject(output, message);
        kryo.writeObject(output, new ArrayList<NodeStorage>());
        kryo.writeObject(output, new ArrayList<RelationshipStorage>());
        return output;
    }

    /**
     * Handle a signature message.
     *
     * @param input          the message.
     * @param messageContext the context.
     * @param kryo           the kryo object.
     */
    private void handleSignatureMessage(final Input input, final MessageContext messageContext, final Kryo kryo)
    {
        //Our own message.
        if (id == messageContext.getSender())
        {
            return;
        }
        final byte[] buffer = input.getBuffer();

        final String decision = kryo.readObject(input, String.class);
        final Long snapShotId = kryo.readObject(input, Long.class);

        final List writeSet = kryo.readObject(input, ArrayList.class);
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

        Log.getLogger().warn("Server: " + id + " Received message to sign with snapShotId: "
                + snapShotId + " of Server "
                + messageContext.getSender()
                + " and decision: " + decision
                + " and a writeSet of the length of: " + localWriteSet.size());

        final int messageLength = kryo.readObject(input, Integer.class);

        final int signatureLength = kryo.readObject(input, Integer.class);
        final byte[] signature = input.readBytes(signatureLength);

        //Not required anymore.
        input.close();

        final RSAKeyLoader rsaLoader = new RSAKeyLoader(messageContext.getSender(), GLOBAL_CONFIG_LOCATION, false);
        final PublicKey key;
        try
        {
            key = rsaLoader.loadPublicKey();
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Unable to load public key on server " + id + " sent by server " + messageContext.getSender(), e);
            return;
        }

        final byte[] message = new byte[messageLength];
        System.arraycopy(buffer, 0, message, 0, messageLength);

        boolean signatureMatches = TOMUtil.verifySignature(key, message, signature);
        if (signatureMatches)
        {
            synchronized (lock)
            {
                storeSignedMessage(snapShotId, signature, messageContext, decision, message);
            }
            return;
        }

        Log.getLogger().warn("Signature doesn't match of message, throwing message away.");
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message at global replica");
        final KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        final Kryo kryo = pool.borrow();
        final Input input = new Input(bytes);

        final String messageType = kryo.readObject(input, String.class);
        Output output = new Output(1, 804800);

        switch (messageType)
        {
            case Constants.READ_MESSAGE:
                Log.getLogger().info("Received Node read message");
                try
                {
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    output = handleNodeRead(input, messageContext, kryo, output);
                }
                catch (Throwable t)
                {
                    Log.getLogger().error("Error on " + Constants.READ_MESSAGE + ", returning empty read", t);
                    output = makeEmptyReadResponse(Constants.READ_MESSAGE, kryo);
                }
                break;
            case Constants.RELATIONSHIP_READ_MESSAGE:
                Log.getLogger().info("Received Relationship read message");
                try
                {
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    output = handleRelationshipRead(input, kryo, output);
                }
                catch (Throwable t)
                {
                    Log.getLogger().error("Error on " + Constants.RELATIONSHIP_READ_MESSAGE + ", returning empty read", t);
                    output = makeEmptyReadResponse(Constants.RELATIONSHIP_READ_MESSAGE, kryo);
                }
                break;
            case Constants.SIGNATURE_MESSAGE:
                Log.getLogger().info("Received signature message");
                handleSignatureMessage(input, messageContext, kryo);
                break;
            case Constants.REGISTER_GLOBALLY_MESSAGE:
                Log.getLogger().info("Received register globally message");
                output.close();
                input.close();
                pool.release(kryo);
                return handleRegisteringSlave(input, kryo);
            case Constants.REGISTER_GLOBALLY_CHECK:
                Log.getLogger().info("Received globally check message");
                output.close();
                input.close();
                pool.release(kryo);
                return handleGlobalRegistryCheck(input, kryo);
            case Constants.COMMIT:
                Log.getLogger().info("Received commit message");
                output.close();
                byte[] result = handleReadOnlyCommit(input, kryo);
                input.close();
                pool.release(kryo);
                Log.getLogger().info("Return it to client, size: " + result.length);
                return result;
            default:
                Log.getLogger().warn("Incorrect operation sent unordered to the server");
                break;
        }

        byte[] returnValue = output.getBuffer();

        Log.getLogger().info("Return it to client, size: " + returnValue.length);

        input.close();
        output.close();
        pool.release(kryo);

        return returnValue;
    }

    private byte[] handleReadOnlyCommit(final Input input, final Kryo kryo)
    {
        final Long timeStamp = kryo.readObject(input, Long.class);
        return executeCommit(kryo, input, timeStamp);
    }

    /**
     * Handle the check for the global registering of a slave.
     *
     * @param input the incoming message.
     * @param kryo  the kryo instance.
     * @return the reply
     */
    private byte[] handleGlobalRegistryCheck(final Input input, final Kryo kryo)
    {
        final Output output = new Output(512);
        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);

        boolean decision = false;
        if (!wrapper.getLocalCLuster().isPrimary() || wrapper.getLocalCLuster().askIfIsPrimary(kryo.readObject(input, Integer.class), kryo.readObject(input, Integer.class), kryo))
        {
            decision = true;
        }

        kryo.writeObject(output, decision);

        final byte[] result = output.getBuffer();
        output.close();
        input.close();
        return result;
    }

    /**
     * This message comes from the local cluster.
     * Will respond true if it can register.
     * Message which handles slaves registering at the global cluster.
     *
     * @param kryo  the kryo instance.
     * @param input the message.
     * @return the message in bytes.
     */
    private byte[] handleRegisteringSlave(final Input input, final Kryo kryo)
    {
        final int localClusterID = kryo.readObject(input, Integer.class);
        final int newPrimary = kryo.readObject(input, Integer.class);
        final int oldPrimary = kryo.readObject(input, Integer.class);

        final ServiceProxy localProxy = new ServiceProxy(1000 + oldPrimary, "local" + localClusterID);

        final Output output = new Output(512);

        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);
        kryo.writeObject(output, newPrimary);

        byte[] result = localProxy.invokeUnordered(output.getBuffer());


        final Output nextOutput = new Output(512);
        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_REPLY);

        final Input answer = new Input(result);
        if (Constants.REGISTER_GLOBALLY_REPLY.equals(answer.readString()))
        {
            kryo.writeObject(nextOutput, answer.readBoolean());
        }

        final byte[] returnBuffer = nextOutput.getBuffer();

        nextOutput.close();
        answer.close();
        localProxy.close();
        output.close();
        return returnBuffer;
        //remove currentView and edit system.config
        //If alright send the result to all remaining global clusters so that they update themselves.
    }

    /**
     * Store the signed message on the server.
     * If n-f messages arrived send it to client.
     *
     * @param snapShotId the snapShotId as key.
     * @param signature  the signature
     * @param context    the message context.
     * @param decision   the decision.
     * @param message    the message.
     */
    private void storeSignedMessage(final Long snapShotId, final byte[] signature, @NotNull final MessageContext context, final String decision, final byte[] message)
    {
        final SignatureStorage signatureStorage;
        if (!signatureStorageMap.containsKey(snapShotId))
        {
            signatureStorage = new SignatureStorage(super.getReplica().getReplicaContext().getStaticConfiguration().getN() - 1, message, decision);
            signatureStorageMap.put(snapShotId, signatureStorage);
            Log.getLogger().info("Replica: " + id + " did not have the transaction prepared. Might be slow or corrupted, message size stored: " + message.length);
        }
        else
        {
            signatureStorage = signatureStorageMap.get(snapShotId);
            if (!signatureStorage.getDecision().equals(decision))
            {
                Log.getLogger().error("Different decision");
            }

            if (signatureStorage.getMessage().length != message.length)
            {
                Log.getLogger().error("Message in signatureStorage: " + signatureStorage.getMessage().length + " message of writing server " + message.length);
            }
        }

        if (!decision.equals(signatureStorage.getDecision()))
        {
            Log.getLogger().warn("Replica: " + id + " did receive a different decision of replica: " + context.getSender() + ". Might be corrupted.");
            return;
        }
        signatureStorage.addSignatures(context.getSender(), signature);

        Log.getLogger().warn("Adding signature to signatureStorage, has: " + signatureStorage.getSignatures().size() + " is: " + signatureStorage.isProcessed());

        if (signatureStorage.hasEnough() && signatureStorage.isProcessed())
        {
            Log.getLogger().warn("Sending update to slave signed by all members: " + snapShotId);
            updateSlave(signatureStorage);
            signatureStorageMap.remove(snapShotId);
            return;
        }
        signatureStorageMap.put(snapShotId, signatureStorage);
    }

    /**
     * Update the slave with a transaction.
     *
     * @param signatureStorage the signatureStorage with message and signatures..
     */
    private void updateSlave(final SignatureStorage signatureStorage)
    {
        if (this.wrapper.getLocalCLuster() != null)
        {
            this.wrapper.getLocalCLuster().propagateUpdate(new SignatureStorage(signatureStorage));
        }
    }

    /**
     * Invoke a message to the global cluster.
     *
     * @param input the input object.
     * @return the response.
     */
    public Output invokeGlobally(final Input input)
    {
        return new Output(proxy.invokeOrdered(input.getBuffer()));
    }

    /**
     * Closes the global cluster and his code.
     */
    public void close()
    {
        super.terminate();
        proxy.close();
    }
}
