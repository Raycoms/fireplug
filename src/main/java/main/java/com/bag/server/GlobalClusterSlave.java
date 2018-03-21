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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.database.SparkseeDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.SignatureStorage;
import org.jetbrains.annotations.NotNull;

import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
     * The internal client used in this server
     */
    private final int idClient;

    /**
     * Cache which holds the signatureStorages for the consistency.
     */
    private final Cache<Long, SignatureStorage> signatureStorageCache = Caffeine.newBuilder().build();

    /**
     * The last sent commit, do not put anything under this number in the signature cache.
     */
    private long lastSent = 0;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private final ServiceProxy proxy;

    /**
     * SignatureStorageCache lock to be sure that we compare correctly.
     */
    private static final Object lock = new Object();

    /**
     * Thread pool for message sending.
     */
    private final ExecutorService service = Executors.newSingleThreadExecutor();

    GlobalClusterSlave(final int id, @NotNull final ServerWrapper wrapper, final ServerInstrumentation instrumentation)
    {
        super(id, GLOBAL_CONFIG_LOCATION, wrapper, instrumentation);
        this.id = id;
        this.idClient = id + 1000;
        this.wrapper = wrapper;
        Log.getLogger().info("Turning on client proxy with id:" + idClient);
        this.proxy = new ServiceProxy(this.idClient, GLOBAL_CONFIG_LOCATION);
        Log.getLogger().info("Turned on global cluster with id:" + id);
    }

    /**
     * Every byte array is one request.
     *
     * @param bytes           the requests.
     * @param messageContexts the contexts.
     * @return the answers of all requests in this batch.
     */
    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts, final boolean noop)
    {
        final byte[][] allResults = new byte[bytes.length][];
        for (int i = 0; i < bytes.length; ++i)
        {
            if (messageContexts != null && messageContexts[i] != null)
            {
                final KryoPool pool = new KryoPool.Builder(super.getFactory()).softReferences().build();
                final Kryo kryo = pool.borrow();
                final Input input = new Input(bytes[i]);

                final String type = kryo.readObject(input, String.class);

                if (Constants.COMMIT_MESSAGE.equals(type))
                {
                    final Long timeStamp = kryo.readObject(input, Long.class);
                    final byte[] result = executeCommit(kryo, input, timeStamp);
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
                signatureStorageCache.put(kryo.readObject(input, Long.class), kryo.readObject(input, SignatureStorage.class));
            }
            catch (final ClassCastException ex)
            {
                Log.getLogger().warn("Unable to restore signatureStoreMap entry: " + i + " at server: " + id, ex);
            }
        }
    }

    @Override
    public Output writeSpecificData(final Output output, final Kryo kryo)
    {
        if (signatureStorageCache == null)
        {
            return output;
        }

        Log.getLogger().warn("Size at global: " + signatureStorageCache.estimatedSize());

        final Map<Long, SignatureStorage> copy = signatureStorageCache.asMap();
        kryo.writeObject(output, copy.size());
        for (final Map.Entry<Long, SignatureStorage> entrySet : copy.entrySet())
        {
            kryo.writeObject(output, entrySet.getKey());
            kryo.writeObject(output, entrySet.getValue());
        }

        return output;
    }

    /**
     * Check for conflicts and unpack things for conflict handle check.
     *
     * @param kryo  the kryo instance.
     * @param input the input.
     * @return the response.
     */
    private synchronized byte[] executeCommit(final Kryo kryo, final Input input, final long timeStamp)
    {
        Log.getLogger().info("Execute commit");
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
        catch (final Exception e)
        {
            Log.getLogger().warn("Couldn't convert received data to sets. Returning abort", e);
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            final byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        if (wrapper.isGloballyVerified() && wrapper.getLocalCluster() != null && !localWriteSet.isEmpty())
        {
            signCommitWithDecisionAndDistribute(localWriteSet, Constants.COMMIT, getGlobalSnapshotId(), kryo, idClient, readSetNode, readsSetRelationship);
        }

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(),
                super.getLatestWritesSet(),
                new ArrayList<>(localWriteSet),
                readSetNode,
                readsSetRelationship,
                timeStamp,
                wrapper.getDataBaseAccess(), wrapper.isMultiVersion()))
        {
            updateCounts(0, 0, 0, 1);

            Log.getLogger()
                    .info("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                            + localWriteSet.size()
                            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            if (!localWriteSet.isEmpty())
            {
                Log.getLogger().info("Aborting of: " + getGlobalSnapshotId() + " localId: " + timeStamp);
            }

            //Send abort to client and abort
            final byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        if (!localWriteSet.isEmpty())
        {
            Log.getLogger().info("Comitting: " + getGlobalSnapshotId() + " localId: " + timeStamp);
            final RSAKeyLoader rsaLoader = new RSAKeyLoader(idClient, GLOBAL_CONFIG_LOCATION, false);
            super.executeCommit(localWriteSet, rsaLoader, idClient, timeStamp);
            if (wrapper.getLocalCluster() != null && !wrapper.isGloballyVerified())
            {
                signCommitWithDecisionAndDistribute(localWriteSet, Constants.COMMIT, getGlobalSnapshotId(), kryo, idClient, readSetNode, readsSetRelationship);
            }
        }
        else
        {
            updateCounts(0, 0, 1, 0);
        }

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        final byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
    }

    /**
     * Check for conflicts and unpack things for conflict handle check.
     *
     * @param kryo  the kryo instance.
     * @param input the input.
     * @return the response.
     */
    private byte[] executeReadOnlyCommit(final Kryo kryo, final Input input, final long timeStamp)
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
        catch (final Exception e)
        {
            Log.getLogger().warn("Couldn't convert received data to sets. Returning abort", e);
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            final byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(),
                super.getLatestWritesSet(),
                localWriteSet,
                readSetNode,
                readsSetRelationship,
                timeStamp,
                wrapper.getDataBaseAccess(), wrapper.isMultiVersion()))
        {
            updateCounts(0, 0, 0, 1);

            Log.getLogger()
                    .info("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                            + localWriteSet.size()
                            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            final byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        updateCounts(0, 0, 1, 0);

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        final byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
    }

    private void signCommitWithDecisionAndDistribute(
            final List<IOperation> localWriteSet, final String decision, final long snapShotId, final Kryo kryo, final int idClient, List<NodeStorage> readSetNode, List<RelationshipStorage> readsSetRelationship)
    {
        Log.getLogger().warn("Sending signed commit to the other global replicas");
        final RSAKeyLoader rsaLoader = new RSAKeyLoader(idClient, GLOBAL_CONFIG_LOCATION, false);

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        final Output output = new Output(0, 100240);

        kryo.writeObject(output, Constants.SIGNATURE_MESSAGE);
        kryo.writeObject(output, decision);
        kryo.writeObject(output, snapShotId);
        kryo.writeObject(output, localWriteSet);
        if(wrapper.isGloballyVerified())
        {
            kryo.writeObject(output, readSetNode);
            kryo.writeObject(output, readsSetRelationship);
        }

        final byte[] message = output.toBytes();
        final byte[] signature;
        try
        {
            signature = TOMUtil.signMessage(rsaLoader.loadPrivateKey(), message);
        }
        catch (final Exception e)
        {
            Log.getLogger().warn("Unable to sign message at server " + getId(), e);
            return;
        }

        SignatureStorage signatureStorage = signatureStorageCache.getIfPresent(snapShotId);
        if (signatureStorage != null)
        {
            if (signatureStorage.getMessage().length != output.toBytes().length)
            {
                Log.getLogger()
                        .warn("Message in signatureStorage: " + signatureStorage.getMessage().length + " message of committing server: " + message.length + "id: "
                                + snapShotId);
            }
        }
        else
        {
            Log.getLogger().warn("Size of message stored is: " + message.length);
            signatureStorage = new SignatureStorage(getReplica().getReplicaContext().getStaticConfiguration().getF() + 1, message, decision);
            signatureStorageCache.put(snapShotId, signatureStorage);
        }

        signatureStorage.setProcessed();
        Log.getLogger().warn("Set processed by global cluster: " + snapShotId + " by: " + idClient);
        signatureStorage.addSignatures(idClient, signature);
        //TODO: only temporary workaround!
        if (wrapper.isGloballyVerified() || signatureStorage.hasEnough())
        {
            Log.getLogger().warn("Sending update to slave signed by all members: " + snapShotId);

            final Output messageOutput = new Output(100096);

            kryo.writeObject(messageOutput, Constants.UPDATE_SLAVE);
            kryo.writeObject(messageOutput, decision);
            kryo.writeObject(messageOutput, snapShotId);
            kryo.writeObject(messageOutput, signatureStorage);

            final MessageThread runnable = new MessageThread(messageOutput.getBuffer());
            //updateSlave(slaveUpdateOutput.getBuffer());
            //updateNextSlave(slaveUpdateOutput.getBuffer());
            service.submit(runnable);
            messageOutput.close();

            signatureStorage.setDistributed();
            signatureStorageCache.put(snapShotId, signatureStorage);
            signatureStorageCache.invalidate(snapShotId);
            lastSent = snapShotId;
        }
        else
        {
            signatureStorageCache.put(snapShotId, signatureStorage);
        }


        kryo.writeObject(output, message.length);
        kryo.writeObject(output, signature.length);
        output.writeBytes(signature);

        if (!wrapper.isGloballyVerified())
        {
            //TODO: might need something similar in the future for global verification.
            proxy.sendMessageToTargets(output.getBuffer(), 0, 0, proxy.getViewManager().getCurrentViewProcesses(), TOMMessageType.UNORDERED_REQUEST);
        }
        output.close();
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
        if (idClient == messageContext.getSender())
        {
            return;
        }
        final byte[] buffer = input.getBuffer();

        final String decision = kryo.readObject(input, String.class);
        final Long snapShotId = kryo.readObject(input, Long.class);
        final List writeSet = kryo.readObject(input, ArrayList.class);
        final ArrayList<IOperation> localWriteSet;

        if(lastSent > snapShotId)
        {
            final SignatureStorage tempStorage = signatureStorageCache.getIfPresent(snapShotId);
            if(tempStorage == null || tempStorage.isDistributed())
            {
                signatureStorageCache.invalidate(snapShotId);
                return;
            }
        }

        try
        {
            localWriteSet = (ArrayList<IOperation>) writeSet;
        }
        catch (final ClassCastException e)
        {
            Log.getLogger().warn("Couldn't convert received signature message.", e);
            return;
        }

        Log.getLogger().info("Server: " + id + " Received message to sign with snapShotId: "
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
        catch (final Exception e)
        {
            Log.getLogger().warn("Unable to load public key on server " + id + " sent by server " + messageContext.getSender(), e);
            return;
        }

        final byte[] message = new byte[messageLength];
        System.arraycopy(buffer, 0, message, 0, messageLength);

        final boolean signatureMatches = TOMUtil.verifySignature(key, message, signature);
        if (signatureMatches)
        {

            storeSignedMessage(snapShotId, signature, messageContext, decision, message, writeSet);
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
        byte[] returnValue;

        try
        {
            switch (messageType)
            {
                case Constants.READ_MESSAGE:
                    Log.getLogger().info("Received Node read message");
                    try
                    {
                        kryo.writeObject(output, Constants.READ_MESSAGE);
                        output = handleNodeRead(input, kryo, output, messageContext.getSender());
                    }
                    catch (final Exception t)
                    {
                        Log.getLogger().error("Error on " + Constants.READ_MESSAGE + ", returning empty read", t);
                        output.close();
                        output = makeEmptyReadResponse(Constants.READ_MESSAGE, kryo);
                    }
                    break;
                case Constants.RELATIONSHIP_READ_MESSAGE:
                    Log.getLogger().info("Received Relationship read message");
                    try
                    {
                        kryo.writeObject(output, Constants.READ_MESSAGE);
                        output = handleRelationshipRead(input, kryo, output, messageContext.getSender());
                    }
                    catch (final Exception t)
                    {
                        Log.getLogger().error("Error on " + Constants.RELATIONSHIP_READ_MESSAGE + ", returning empty read", t);
                        output = makeEmptyReadResponse(Constants.RELATIONSHIP_READ_MESSAGE, kryo);
                    }
                    break;
                case Constants.SIGNATURE_MESSAGE:
                    if (wrapper.getLocalCluster() != null)
                    {
                        handleSignatureMessage(input, messageContext, kryo);
                    }
                    break;
                case Constants.REGISTER_GLOBALLY_MESSAGE:
                    Log.getLogger().info("Received register globally message");
                    output.close();
                    input.close();
                    pool.release(kryo);
                    return handleRegisteringSlave(input, kryo);
                case Constants.COMMIT:
                    Log.getLogger().info("Received commit message: " + input.getBuffer().length);
                    if (wrapper.getDataBaseAccess() instanceof SparkseeDatabaseAccess)
                    {
                        input.close();
                        pool.release(kryo);
                        return new byte[] {0};
                    }

                    final byte[] result;
                    result = handleReadOnlyCommit(input, kryo);
                    input.close();
                    pool.release(kryo);
                    Log.getLogger().info("Return it to client, size: " + result.length);
                    return result;
                default:
                    Log.getLogger().warn("Incorrect operation sent unordered to the server");
                    break;
            }
            returnValue = output.getBuffer();
        }
        finally
        {
            output.close();
        }

        Log.getLogger().info("Return it to client, size: " + returnValue.length);

        input.close();
        output.close();
        pool.release(kryo);

        return returnValue;
    }

    private byte[] handleReadOnlyCommit(final Input input, final Kryo kryo)
    {
        final Long timeStamp = kryo.readObject(input, Long.class);
        return executeReadOnlyCommit(kryo, input, timeStamp);
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
    @SuppressWarnings("squid:S2095")
    private byte[] handleRegisteringSlave(final Input input, final Kryo kryo)
    {
        final int localClusterID = kryo.readObject(input, Integer.class);
        final int newPrimary = kryo.readObject(input, Integer.class);
        final int oldPrimary = kryo.readObject(input, Integer.class);

        final ServiceProxy localProxy = new ServiceProxy(1000 + oldPrimary, "local" + localClusterID);

        final Output output = new Output(512);

        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);
        kryo.writeObject(output, newPrimary);

        final byte[] result = localProxy.invokeUnordered(output.getBuffer());


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
    private void storeSignedMessage(
            final Long snapShotId,
            final byte[] signature,
            @NotNull final MessageContext context,
            final String decision,
            final byte[] message,
            final List<IOperation> writeSet)
    {
        final SignatureStorage signatureStorage;

        synchronized (lock)
        {
            final SignatureStorage tempStorage = signatureStorageCache.getIfPresent(snapShotId);
            signatureStorageCache.invalidate(snapShotId);
            if (tempStorage == null)
            {
                signatureStorage = new SignatureStorage(super.getReplica().getReplicaContext().getStaticConfiguration().getF() + 1, message, decision);
                Log.getLogger().info("Replica: " + id + " did not have the transaction prepared. Might be slow or corrupted, message size stored: " + message.length);
            }
            else
            {
                signatureStorage = tempStorage;
            }

            if (signatureStorage.getMessage().length != message.length)
            {
                Log.getLogger().info("Message in signatureStorage: " + signatureStorage.getMessage().length + " message of writing server "
                        + message.length + " ws: " + writeSet.size() + " id: " + snapShotId);
            }

            if (!decision.equals(signatureStorage.getDecision()))
            {
                Log.getLogger().warn("Replica: " + id + " did receive a different decision of replica: " + context.getSender() + ". Might be corrupted.");
                return;
            }
            signatureStorage.addSignatures(context.getSender(), signature);

            Log.getLogger().info("Adding signature to signatureStorage, has: " + signatureStorage.getSignatures().size() + " is: " + signatureStorage.isProcessed()
                    + " by: " + context.getSender());

            if (signatureStorage.hasEnough())
            {
                Log.getLogger().info("Sending update to slave signed by all members: " + snapShotId);
                if (signatureStorage.isProcessed())
                {
                    final Output messageOutput = new Output(100096);
                    final KryoPool pool = new KryoPool.Builder(super.getFactory()).softReferences().build();
                    final Kryo kryo = pool.borrow();

                    kryo.writeObject(messageOutput, Constants.UPDATE_SLAVE);
                    kryo.writeObject(messageOutput, decision);
                    kryo.writeObject(messageOutput, snapShotId);
                    kryo.writeObject(messageOutput, signatureStorage);

                    final MessageThread runnable = new MessageThread(messageOutput.getBuffer());
                    //updateSlave(slaveUpdateOutput.getBuffer());
                    //updateNextSlave(slaveUpdateOutput.getBuffer());
                    service.submit(runnable);
                    messageOutput.close();
                    pool.release(kryo);
                    lastSent = snapShotId;
                    signatureStorage.setDistributed();
                }
            }

            if(!signatureStorage.isDistributed())
            {
                signatureStorageCache.put(snapShotId, signatureStorage);
            }
        }
    }

    private void updateSlave(final byte[] message)
    {
        if (wrapper.getLocalCluster() != null)
        {
            Log.getLogger().info("Notifying local cluster!");
            wrapper.getLocalCluster().propagateUpdate(message);
        }
    }

    @Override
    public void putIntoWriteSet(final long currentSnapshot, final List<IOperation> localWriteSet)
    {
        super.putIntoWriteSet(currentSnapshot, localWriteSet);
        if (wrapper.getLocalCluster() != null)
        {
            wrapper.getLocalCluster().putIntoWriteSet(currentSnapshot, localWriteSet);
        }
    }

    /**
     * Invoke a message to the global cluster.
     *
     * @param input the input object.
     * @return the response.
     */
    Output invokeGlobally(final Input input)
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

    private class MessageThread implements Runnable
    {
        private final byte[] message;
        MessageThread(final byte[] message)
        {
            this.message = message;
        }

        @Override
        public void run()
        {
            updateSlave(message);
        }

        /**
         * Update the slave with a transaction.
         *
         * @param message the message to propagate.
         */
        private void updateSlave(final byte[] message)
        {
            if (wrapper.getLocalCluster() != null)
            {
                Log.getLogger().info("Notifying local cluster!");
                wrapper.getLocalCluster().propagateUpdate(message);
            }
        }
    }
}
