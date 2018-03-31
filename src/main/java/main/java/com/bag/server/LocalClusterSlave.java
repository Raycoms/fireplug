package main.java.com.bag.server;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.util.TOMUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.SignatureStorage;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 *
 */
public class LocalClusterSlave extends AbstractRecoverable
{
    /**
     * Name of the location of the global config.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global/config";

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

    /**
     * Queue to catch messages out of order.
     */
    private final Map<Long, List<IOperation>> buffer = new TreeMap<>();

    /**
     * Lock to lock the update slave execution to order the execution correctly.
     */
    private final Object lock = new Object();

    /**
     * Public constructor used to create a local cluster slave.
     *
     * @param id             its unique id in the local cluster.
     * @param wrapper        its ordering wrapper.
     * @param localClusterId the id of the cluster (the id of the starting primary in the global cluster).
     */
    public LocalClusterSlave(final int id, @NotNull final ServerWrapper wrapper, final int localClusterId, final ServerInstrumentation instrumentation)
    {
        super(id, String.format(LOCAL_CONFIG_LOCATION, localClusterId), wrapper, instrumentation);
        this.id = id;
        this.wrapper = wrapper;
        this.proxy = new ServiceProxy(1000 + id, String.format(LOCAL_CONFIG_LOCATION, localClusterId));
        Log.getLogger().info("Turned on local cluster with id: " + id);
    }

    /**
     * Set the local cluster instance to primary.
     *
     * @param isPrimary true if so.
     */
    void setPrimary(final boolean isPrimary)
    {
        if (isPrimary)
        {
            primaryId = id;
        }
        this.isPrimary = isPrimary;
    }

    /**
     * Set the id of the primary global cluster.
     *
     * @param primaryGlobalClusterId the id.
     */
    void setPrimaryGlobalClusterId(final int primaryGlobalClusterId)
    {
        this.primaryGlobalClusterId = primaryGlobalClusterId;
    }

    /**
     * Check if the local cluster slave is a primary.
     *
     * @return true if so.
     */
    boolean isPrimary()
    {
        return isPrimary;
    }

    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts, final boolean bim)
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
                    final byte[] result = handleReadOnlyCommit(input, kryo);
                    pool.release(kryo);
                    allResults[i] = result;
                }
                else if (Constants.UPDATE_SLAVE.equals(type))
                {
                    final Output output = new Output(0, 1024);
                    Log.getLogger().info("Received update slave message");
                    synchronized (lock)
                    {
                        handleSlaveUpdateMessage(input, output, kryo);
                    }
                    allResults[i] = output.getBuffer();
                    output.close();
                    input.close();
                }
                else
                {
                    Log.getLogger().warn("Return empty bytes for message type: " + type);
                    allResults[i] = makeEmptyAbortResult();
                    updateCounts(0, 0, 0, 1);
                }
            }
            else
            {
                Log.getLogger().warn("Received message with empty context!");
                allResults[i] = makeEmptyAbortResult();
                updateCounts(0, 0, 0, 1);
            }
        }

        return allResults;
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message");
        final KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        final Kryo kryo = pool.borrow();
        final Input input = new Input(bytes);
        final String reason = kryo.readObject(input, String.class);
        byte[] returnValue;
        Output output = new Output(0, 400240);
        try
        {
            switch (reason)
            {
                case Constants.READ_MESSAGE:
                    Log.getLogger().info("Received Node read message");
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    output = handleNodeRead(input, kryo, output, messageContext.getSender());
                    break;
                case Constants.RELATIONSHIP_READ_MESSAGE:
                    Log.getLogger().info("Received Relationship read message");
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    output = handleRelationshipRead(input, kryo, output, messageContext.getSender());
                    break;
                case Constants.GET_PRIMARY:
                    Log.getLogger().info("Received GetPrimary message");
                    kryo.writeObject(output, Constants.GET_PRIMARY);
                    output = handleGetPrimaryMessage(messageContext, output, kryo);
                    break;
                case Constants.COMMIT:
                    Log.getLogger().info("Received commit message: " + input.getBuffer().length);
                    output.close();
                    final byte[] result = handleReadOnlyCommit(input, kryo);
                    input.close();
                    pool.release(kryo);
                    Log.getLogger().info("Return it to client: " + input.getBuffer().length + ", size: " + result.length);
                    return result;
                case Constants.REGISTER_GLOBALLY_MESSAGE:
                    Log.getLogger().info("Received register globally message");
                    output = handleRegisterGloballyMessage(input, output, messageContext, kryo);
                    break;
                case Constants.UPDATE_SLAVE:
                    Log.getLogger().info("Received update slave message");
                    synchronized (lock)
                    {
                        try
                        {
                            output = handleSlaveUpdateMessage(input, output, kryo);
                            if (output == null)
                            {
                                Log.getLogger().warn("Error, error, null output detected");
                            }
                        }
                        catch(final Exception ex)
                        {
                            Log.getLogger().warn("Local: ", ex);
                        }
                    }
                    input.close();
                    return new byte[0];
                default:
                    Log.getLogger().warn("Incorrect operation sent unordered to the server");
                    output.close();
                    input.close();
                    return new byte[0];
            }
            returnValue = output.getBuffer();
        }
        finally
        {
            output.close();
        }

        Log.getLogger().info("Return it to sender, size: " + returnValue.length);

        input.close();
        pool.release(kryo);

        return returnValue;
    }

    private byte[] handleReadOnlyCommit(final Input input, final Kryo kryo)
    {
        final Long timeStamp = kryo.readObject(input, Long.class);
        return executeReadOnlyCommit(kryo, input, timeStamp);
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

    /**
     * Check if the primary is correct.
     *
     * @param input          the input.
     * @param output         the presumed output.
     * @param messageContext the message context.
     * @param kryo           the kryo object.
     * @return output obejct with decision.
     */
    private Output handleRegisterGloballyMessage(final Input input, final Output output, final MessageContext messageContext, final Kryo kryo)
    {
        final int oldPrimary = kryo.readObject(input, Integer.class);
        final int newPrimary = kryo.readObject(input, Integer.class);

        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_REPLY);
        if (messageContext.getLeader() == newPrimary)
        {
            kryo.writeObject(output, true);
        }
        else
        {
            kryo.writeObject(output, false);
        }

        if (messageContext.getLeader() == oldPrimary)
        {
            Log.getLogger().warn("Slave: " + newPrimary + "tried to register as new primary.");
        }
        return output;
    }

    /**
     * Handles a get primary message.
     *
     * @param messageContext the message context.
     * @param output         write info to.
     * @param kryo           the kryo instance.
     * @return sends the primary to the people.
     */
    private Output handleGetPrimaryMessage(final MessageContext messageContext, final Output output, final Kryo kryo)
    {
        if (isPrimary())
        {
            kryo.writeObject(output, id);
        }
        if (isPrimary())
        {
            kryo.writeObject(output, primaryId);
        }
        return output;
    }

    private Output handleSlaveUpdateMessage(final Input input, final Output output, final Kryo kryo)
    {
        //Not required. Is primary already dealt with it.
        if (wrapper.getGlobalCluster() != null)
        {
            kryo.writeObject(output, true);
            return output;
        }

        final String decision = kryo.readObject(input, String.class);
        final long snapShotId = kryo.readObject(input, Long.class);
        final long lastKey = getGlobalSnapshotId();

        Log.getLogger().info("Received update slave message with decision: " + decision);

        if (lastKey > snapShotId)
        {
            //Received a message which has been committed in the past already.
            kryo.writeObject(output, true);
            return output;
        }
        else if (lastKey == snapShotId)
        {
            Log.getLogger().info("Received already committed transaction.");
            kryo.writeObject(output, true);
            return output;
        }

        final SignatureStorage storage;

        try
        {
            storage = kryo.readObject(input, SignatureStorage.class);
        }
        catch (final ClassCastException exp)
        {
            Log.getLogger().warn("Unable to cast to SignatureStorage, something went wrong badly.", exp);
            kryo.writeObject(output, false);
            return output;
        }
        final int consensusId = kryo.readObject(input, Integer.class);

        final Input messageInput = new Input(storage.getMessage());

        kryo.readObject(messageInput, String.class);
        kryo.readObject(messageInput, String.class);

        kryo.readObject(messageInput, Long.class);

        final List writeSet = kryo.readObject(messageInput, ArrayList.class);
        List readsSetNodeX = new ArrayList<>();
        List readsSetRelationshipX = new ArrayList<>();

        if (wrapper.isGloballyVerified())
        {
            readsSetNodeX = kryo.readObject(messageInput, ArrayList.class);
            readsSetRelationshipX = kryo.readObject(messageInput, ArrayList.class);
        }
        final ArrayList<IOperation> localWriteSet;
        ArrayList<NodeStorage> readSetNode = new ArrayList<>();
        ArrayList<RelationshipStorage> readsSetRelationship = new ArrayList<>();

        messageInput.close();
        try
        {
            localWriteSet = (ArrayList<IOperation>) writeSet;
            if (wrapper.isGloballyVerified() && !readsSetNodeX.isEmpty() && !readsSetRelationshipX.isEmpty())
            {
                readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
                readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            }
        }
        catch (final ClassCastException e)
        {
            Log.getLogger().warn("Couldn't convert received signature message.", e);
            kryo.writeObject(output, false);
            return output;
        }

        if (!wrapper.isGloballyVerified())
        {
            int matchingSignatures = 0;
            for (final Map.Entry<Integer, byte[]> entry : storage.getSignatures().entrySet())
            {
                final RSAKeyLoader rsaLoader = new RSAKeyLoader(entry.getKey(), GLOBAL_CONFIG_LOCATION, false);
                try
                {
                    if (!TOMUtil.verifySignature(rsaLoader.loadPublicKey(), storage.getMessage(), entry.getValue()))
                    {
                        Log.getLogger().warn("Signature of server: " + entry.getKey() + " doesn't match");
                        Log.getLogger().warn(Arrays.toString(storage.getMessage()) + " : " + Arrays.toString(entry.getValue()));
                    }
                    else
                    {
                        Log.getLogger().info("Signature matches of server: " + entry.getKey());
                        matchingSignatures++;
                    }
                }
                catch (final Exception e)
                {
                    Log.getLogger().warn("Unable to load public key on server " + id + " of server: " + entry.getKey(), e);
                    kryo.writeObject(output, false);
                    return output;
                }
            }

            if (matchingSignatures < 2)
            {
                Log.getLogger().info("Something went incredibly wrong. Transaction came without correct signatures from the primary at localCluster: "
                                + wrapper.getLocalClusterSlaveId());
                kryo.writeObject(output, false);
                return output;
            }
            Log.getLogger().info("All: " + matchingSignatures + " signatures are correct, started to commit now!");
        }

        //Code to dynamically reconfigure the local cluster!
        /*if (getGlobalSnapshotId() == 1000 && id == 2 && localClusterId == 0)
        {
            Log.getLogger().warn("Instantiating new global cluster");

            final Thread t = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {

                        final Thread t = new Thread(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                wrapper.initNewGlobalClusterInstance();
                            }
                        });
                        t.start();
                        Thread.sleep(100);
                        VMServices.main(new String[] {"4", "172.16.52.8", "11340"});
                    }
                    catch (final InterruptedException e)
                    {
                        Log.getLogger().warn("Error instantiating new Replica", e);
                    }
                }
            });
            t.start();
        }*/

        if (lastKey + 1 == snapShotId && Constants.COMMIT.equals(decision))
        {
            if (wrapper.isGloballyVerified() && !ConflictHandler.checkForConflict(super.getGlobalWriteSet(),
                    super.getLatestWritesSet(),
                    new ArrayList<>(localWriteSet),
                    readSetNode,
                    readsSetRelationship,
                    snapShotId,
                    wrapper.getDataBaseAccess(), wrapper.isMultiVersion()))
            {
                Log.getLogger()
                        .warn("Found conflict, returning abort with timestamp: " + snapShotId + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                                + localWriteSet.size()
                                + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
                kryo.writeObject(output, false);
                return output;
            }
            final RSAKeyLoader rsaLoader = new RSAKeyLoader(id, GLOBAL_CONFIG_LOCATION, false);
            executeCommit(localWriteSet, rsaLoader, id, snapShotId, consensusId);

            long requiredKey = lastKey + 1;
            while (buffer.containsKey(requiredKey))
            {
                executeCommit(buffer.remove(requiredKey), rsaLoader, id, snapShotId, consensusId);
                requiredKey++;
            }

            kryo.writeObject(output, true);
            return output;
        }
        buffer.put(snapShotId, localWriteSet);
        Log.getLogger().info("Something went wrong, missing a message: " + snapShotId + " with decision: " + decision + " lastKey: " + lastKey + " adding to buffer");
        kryo.writeObject(output, true);
        return output;
    }

    @Override
    public void putIntoWriteSet(final long currentSnapshot, final List<IOperation> localWriteSet)
    {
        super.putIntoWriteSet(currentSnapshot, localWriteSet);
        setGlobalSnapshotId(currentSnapshot);
    }

    @Override
    void readSpecificData(final Input input, final Kryo kryo)
    {
        isPrimary = false;
        primaryGlobalClusterId = kryo.readObject(input, Integer.class);
    }

    @Override
    public Output writeSpecificData(final Output output, final Kryo kryo)
    {
        kryo.writeObject(output, primaryGlobalClusterId);
        return output;
    }


    /**
     * Send this update to all other replicas.
     * @param message the message.
     */
    public void propagateUpdate(final byte[] message)
    {
        while(proxy.invokeUnordered(message) == null)
        {
            Log.getLogger().warn("Slave update failed, no response, trying again!");
        }
    }
}
