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
import main.java.com.bag.instrumentations.ServerInstrumentation;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.reconfiguration.sensors.BftDetectionSensor;
import main.java.com.bag.reconfiguration.sensors.LoadSensor;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.LocalSlaveUpdateStorage;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.SignatureStorage;
import org.jetbrains.annotations.NotNull;

import java.io.DataOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
     * Importance of the CPU when electing a new primary.
     */
    private static final double CPU_IMPORTANCE_MULTIPLIER = 10.0;

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
    private int primaryId;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private ServiceProxy proxy;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private ServiceProxy crashProxy;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private ServiceProxy loadProxy;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private ServiceProxy bftProxy;

    /**
     * Queue to catch messages out of order.
     */
    private final Map<Long, LocalSlaveUpdateStorage> buffer = new TreeMap<>();

    /**
     * Timer object to execute functions in intervals
     */
    private final Timer timer = new Timer();

    /**
     * If the local cluster member had to jump in for its primary.
     */
    private boolean substitutesPrimary = false;

    /**
     * Hashmap which has the information about all local node performances.
     */
    private final HashMap<Integer, LoadSensor.LoadDesc> performanceMap = new HashMap<>();

    /**
     * If currently a new primary is being elected.
     */
    private boolean currentlyElectingNewPrimary = false;

    /**
     * Public constructor used to create a local cluster slave.
     *
     * @param id             its unique id in the local cluster.
     * @param wrapper        its ordering wrapper.
     * @param localClusterId the id of the cluster (the id of the starting primary in the global cluster).
     */
    public LocalClusterSlave(final int id, @NotNull final ServerWrapper wrapper, final int localClusterId, final ServerInstrumentation instrumentation)
    {
        super(id, String.format(LOCAL_CONFIG_LOCATION, localClusterId), wrapper, instrumentation, 0);
        this.id = id;
        this.proxy = new ServiceProxy(1000 + id, String.format(LOCAL_CONFIG_LOCATION, localClusterId));
        this.primaryId = 0;
        Log.getLogger().info("Turned on local cluster with id: " + id);
        final int positionToCheck;
        if (this.id + 1 >= proxy.getViewManager().getCurrentViewN())
        {
            positionToCheck = 0;
        }
        else
        {
            positionToCheck = this.id + 1;
        }

        //final KryoPool pool = new KryoPool.Builder(super.getFactory()).softReferences().build();
        //crashProxy = new ServiceProxy(3000 + id, String.format(LOCAL_CONFIG_LOCATION, localClusterId));
        //loadProxy = new ServiceProxy(2000 + id, String.format(LOCAL_CONFIG_LOCATION, localClusterId));
        //timer.scheduleAtFixedRate(new CrashDetectionSensor(positionToCheck, crashProxy, String.format(LOCAL_CONFIG_LOCATION, localClusterId), id, pool.borrow(), localClusterId, this), 10000, 8000);

        //bftProxy = new ServiceProxy(4000 + id, String.format(LOCAL_CONFIG_LOCATION, localClusterId));
        //timer.scheduleAtFixedRate(new BftDetectionSensor(crashProxy, String.format(LOCAL_CONFIG_LOCATION, localClusterId), id, pool.borrow(), localClusterId, this), 10000, 5000 + (id * 1000));
        //timer.scheduleAtFixedRate(new LoadSensor(pool.borrow(), loadProxy, id, wrapper.getDataBaseAccess().getName()), 10000, 10000);

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
                    final Output output;
                    Log.getLogger().error("Received update slave message ordered");
                    output = handleSlaveUpdateMessage(input, new Output(0, 1024), kryo);
                    Log.getLogger().error("Leaving update slave message ordered");
                    allResults[i] = output.toBytes();
                    output.close();
                    input.close();
                }
                else if (Constants.PRIMARY_ELECTION_MESSAGE.equals(type))
                {
                    final Output output;
                    Log.getLogger().error("Received primary election message ordered");
                    output = handlePrimaryElection(input, kryo, false);
                    Log.getLogger().error("Leaving primary election message ordered");
                    allResults[i] = output.toBytes();
                    output.close();
                    input.close();
                }
                else if (Constants.BFT_PRIMARY_ELECTION_MESSAGE.equals(type))
                {
                    final Output output;
                    Log.getLogger().error("Received bft primary election message ordered");
                    output = handlePrimaryElection(input, kryo, true);
                    Log.getLogger().error("Leaving bft primary election message ordered");
                    allResults[i] = output.toBytes();
                    output.close();
                    input.close();
                }
                else if (Constants.PERFORMANCE_UPDATE_MESSAGE.equals(type))
                {
                    Log.getLogger().info("Received performance update message");
                    handlePerformanceUpdateMessage(input, kryo);
                    input.close();
                    allResults[i] = new byte[0];
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

    /**
     * Method to handle the primary election.
     *
     * @param input the input.
     * @param kryo  the kryo object.
     * @return the output to respond.
     */
    private Output handlePrimaryElection(final Input input, final Kryo kryo, final boolean bft)
    {
        final int failedReplica = kryo.readObject(input, Integer.class);
        final Output output = new Output(0, 1024);

        if (checkIfFailedReplicaIsGone(failedReplica) && !bft)
        {
            kryo.writeObject(output, failedReplica);
            return output;
        }

        final LoadSensor.LoadDesc failedLoad = performanceMap.get(failedReplica);
        LoadSensor.LoadDesc best = null;
        int leadingReplica = -1;
        for (final Map.Entry<Integer, LoadSensor.LoadDesc> entry : performanceMap.entrySet())
        {
            if (entry.getKey() != failedReplica && failedLoad.getDb().equalsIgnoreCase(entry.getValue().getDb()))
            {
                final LoadSensor.LoadDesc thisDesc = entry.getValue();
                if (best == null)
                {
                    best = thisDesc;
                    leadingReplica = entry.getKey();
                }
                else
                {
                    double score = (best.getCpuUsage() - thisDesc.getCpuUsage()) * CPU_IMPORTANCE_MULTIPLIER;
                    score += (best.getAllocatedMemory() - thisDesc.getAllocatedMemory()) / 800000.0;
                    score += (best.getMaxMemory() - thisDesc.getMaxMemory()) / 800000.0;
                    score += (best.getFreeMemory() - thisDesc.getFreeMemory()) / 800000.0;

                    if (score < 0)
                    {
                        best = thisDesc;
                        leadingReplica = entry.getKey();
                    }
                }
            }
        }

        if (id == leadingReplica)
        {
            Log.getLogger().error("Instantiating new global cluster");
            substitutesPrimary = true;
            final Thread t = new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    wrapper.initNewGlobalClusterInstance(lastBatch);
                }
            });
            t.start();
        }

        Log.getLogger().warn("Decided on: " + leadingReplica);
        kryo.writeObject(output, leadingReplica);
        return output;
    }

    /**
     * Checks if the server really has to update the primary.
     *
     * @param failedReplica the id of the supposently saved replica.
     * @return true if so.
     */
    private boolean checkIfFailedReplicaIsGone(final int failedReplica)
    {
        final InetSocketAddress address = proxy.getViewManager().getCurrentView().getAddress(failedReplica);
        boolean needsReconfiguration = false;

        if (address == null)
        {
            return true;
        }

        try (Socket socket = new Socket(address.getHostName(), address.getPort()))
        {
            new DataOutputStream(socket.getOutputStream()).writeInt(id + 1);
            Log.getLogger().warn("Connection established in primary check");
        }
        catch (final ConnectException ex)
        {
            Log.getLogger().warn("Connection exception in primary check");
            if (ex.getMessage().contains("refused"))
            {
                needsReconfiguration = true;
            }
        }
        catch (final Exception ex)
        {
            //This here is normal in the global cluster, let's ignore this.
            Log.getLogger().warn("Something went wrong", ex);
        }
        return !needsReconfiguration;
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        final KryoPool pool;
        final Kryo kryo;
        final Input input;
        final byte[] returnValue;

        try (Output output = new Output(0, 400240))
        {
            Log.getLogger().info("Received unordered message");
            pool = new KryoPool.Builder(getFactory()).softReferences().build();
            kryo = pool.borrow();
            input = new Input(bytes);
            final String reason = kryo.readObject(input, String.class);

            switch (reason)
            {
                case Constants.READ_MESSAGE:
                    Log.getLogger().info("Received Node read message");
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    handleNodeRead(input, kryo, output, messageContext.getSender());
                    break;
                case Constants.RELATIONSHIP_READ_MESSAGE:
                    Log.getLogger().info("Received Relationship read message");
                    kryo.writeObject(output, Constants.READ_MESSAGE);
                    handleRelationshipRead(input, kryo, output, messageContext.getSender());
                    break;
                case Constants.GET_PRIMARY:
                    Log.getLogger().info("Received GetPrimary message");
                    kryo.writeObject(output, Constants.GET_PRIMARY);
                    handleGetPrimaryMessage(messageContext, output, kryo);
                    break;
                case Constants.COMMIT:
                    Log.getLogger().info("Received commit message: " + input.getBuffer().length);
                    final byte[] result = handleReadOnlyCommit(input, kryo);
                    input.close();
                    pool.release(kryo);
                    Log.getLogger().info("Return it to client: " + input.getBuffer().length + ", size: " + result.length);
                    return result;
                case Constants.REGISTER_GLOBALLY_MESSAGE:
                    Log.getLogger().info("Received register globally message");
                    handleRegisterGloballyMessage(input, output, messageContext, kryo);
                    break;
                case Constants.UPDATE_SLAVE:
                    Log.getLogger().info("Received update slave message");
                    handleSlaveUpdateMessage(input, output, kryo);
                    input.close();
                    break;
                default:
                    Log.getLogger().error("Incorrect operation sent unordered to the server");
                    input.close();
                    return new byte[0];
            }
            returnValue = output.toBytes();
            Log.getLogger().info("Return it to sender, size: " + returnValue.length);
        }
        input.close();
        pool.release(kryo);
        return returnValue;
    }

    /**
     * Handling of performance update messages.
     * Stores the performance update in the performance tracking hashmap.
     *
     * @param input the input with the data.
     * @param kryo  the kryo instance.
     */
    private void handlePerformanceUpdateMessage(final Input input, final Kryo kryo)
    {
        final LoadSensor.LoadDesc loadDesc = kryo.readObject(input, LoadSensor.LoadDesc.class);
        final int sender = kryo.readObject(input, Integer.class);
        performanceMap.put(sender, loadDesc);
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
            Log.getLogger().error("Couldn't convert received data to sets. Returning abort", e);
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            final byte[] returnBytes = output.toBytes();
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
            final byte[] returnBytes = output.toBytes();
            output.close();
            return returnBytes;
        }

        updateCounts(0, 0, 1, 0);

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        final byte[] returnBytes = output.toBytes();
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
            Log.getLogger().error("Slave: " + newPrimary + "tried to register as new primary.");
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

    @NotNull
    private synchronized Output handleSlaveUpdateMessage(final Input input, @NotNull final Output output, final Kryo kryo)
    {
        proxy.getViewManager().updateCurrentViewFromRepository();
        proxy.getCommunicationSystem().updateConnections();

        //Not required. Is primary already dealt with it.
        if (wrapper.getGlobalCluster() != null)
        {
            kryo.writeObject(output, true);
            return output;
        }

        final String decision = kryo.readObject(input, String.class);
        final long snapShotId = kryo.readObject(input, Long.class);
        final long timeStamp = wrapper.isGloballyVerified() ? kryo.readObject(input, Long.class) : snapShotId;
        final long lastKey = getGlobalSnapshotId();

        Log.getLogger().info("Received update slave message with decision: " + decision);

        if (lastKey > snapShotId)
        {
            Log.getLogger().warn("Throwing away, incoming snapshotId: " + snapShotId + " smaller than existing: " + lastKey);
            //Received a message which has been committed in the past already.
            kryo.writeObject(output, true);
            return output;
        }
        else if (lastKey == snapShotId && !wrapper.isGloballyVerified())
        {
            Log.getLogger().warn("Received already committed transaction.");
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
            Log.getLogger().error("Unable to cast to SignatureStorage, something went wrong badly.", exp);
            kryo.writeObject(output, false);
            return output;
        }
        final int consensusId = kryo.readObject(input, Integer.class);
        lastBatch = consensusId;

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
            if (wrapper.isGloballyVerified())
            {
                if (!readsSetNodeX.isEmpty())
                {
                    readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
                }

                if (!readsSetRelationshipX.isEmpty())
                {
                    readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
                }
            }
        }
        catch (final ClassCastException e)
        {
            Log.getLogger().error("Couldn't convert received signature message.", e);
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
                        Log.getLogger().error("Signature of server: " + entry.getKey() + " doesn't match");
                        Log.getLogger().error(Arrays.toString(storage.getMessage()) + " : " + Arrays.toString(entry.getValue()));
                    }
                    else
                    {
                        Log.getLogger().info("Signature matches of server: " + entry.getKey());
                        matchingSignatures++;
                    }
                }
                catch (final Exception e)
                {
                    Log.getLogger().error("Unable to load public key on server " + id + " of server: " + entry.getKey(), e);
                    kryo.writeObject(output, false);
                    return output;
                }
            }

            if (matchingSignatures < 2)
            {
                Log.getLogger().error("Something went incredibly wrong. Transaction came without correct signatures from the primary at localCluster: "
                        + wrapper.getLocalClusterSlaveId());
                kryo.writeObject(output, false);
                return output;
            }
            Log.getLogger().info("All: " + matchingSignatures + " signatures are correct, started to commit now!");
        }

        buffer.put(snapShotId, new LocalSlaveUpdateStorage(localWriteSet, readSetNode, readsSetRelationship, timeStamp));
        if (lastKey + 1 == snapShotId)
        {
            long requiredKey = lastKey + 1;
            while (buffer.containsKey(requiredKey))
            {
                final LocalSlaveUpdateStorage updateStorage = buffer.remove(requiredKey);
                if (wrapper.isGloballyVerified() && !ConflictHandler.checkForConflict(
                        super.getGlobalWriteSet(),
                        super.getLatestWritesSet(),
                        new ArrayList<>(updateStorage.getLocalWriteSet()),
                        updateStorage.getReadSetNode(),
                        updateStorage.getReadsSetRelationship(),
                        updateStorage.getSnapShotId(),
                        wrapper.getDataBaseAccess(), wrapper.isMultiVersion()))
                {
                    Log.getLogger()
                            .info("Found conflict, returning abort with timestamp: " + snapShotId + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                                    + localWriteSet.size()
                                    + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
                    kryo.writeObject(output, true);
                    updateCounts(0, 0, 0, 1);
                    return output;
                }
                final RSAKeyLoader rsaLoader = new RSAKeyLoader(id, GLOBAL_CONFIG_LOCATION, false);
                executeCommit(updateStorage.getLocalWriteSet(), rsaLoader, id, updateStorage.getSnapShotId(), consensusId);
                buffer.remove(requiredKey);
                requiredKey++;
            }

            kryo.writeObject(output, true);
            return output;
        }

        Log.getLogger().info("Something went wrong, missing a message: " + snapShotId + " with decision: " + decision + " lastKey: " + lastKey + " adding to buffer");
        if (buffer.size() % 200 == 0)
        {
            Log.getLogger().error("Missing more than: " + buffer.size() + " messages, something is broken!" + lastKey);
        }
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
     * Closes the local cluster and his code.
     */
    public void close()
    {
        Log.getLogger().warn("Closing local replica: " + id);
        timer.cancel();
        timer.purge();
        if (proxy != null)
        {
            proxy.close();
            proxy = null;
        }
        if (loadProxy != null)
        {
            loadProxy.close();
            loadProxy = null;
        }
        if (crashProxy != null)
        {
            crashProxy.close();
            crashProxy = null;
        }
        if (bftProxy != null)
        {
            bftProxy.close();
            bftProxy = null;
        }
        super.terminate();
    }

    /**
     * Check if the local cluster member substitutes its primary.
     *
     * @return true if so.
     */
    public boolean isPrimarySubstitute()
    {
        return substitutesPrimary;
    }

    /**
     * Send this update to all other replicas.
     *
     * @param message the message.
     */
    public void propagateUpdate(final byte[] message)
    {
        /*while (proxy.invokeUnordered(message) == null)
        {
            /**
             * Try again.
             */
        //}
        proxy.sendMessageToTargets(message, 0, 0, proxy.getViewManager().getCurrentViewProcesses(), TOMMessageType.UNORDERED_REQUEST);
    }

    /**
     * Method to check if currently a new primary is being elected.
     *
     * @return true if so.
     */
    public boolean isCurrentlyElectingNewPrimary()
    {
        return currentlyElectingNewPrimary;
    }

    /**
     * Method to set that a new primary elecetion started.
     *
     * @param decision if so.
     */
    public void setIsCurrentlyElectingNewPrimary(final boolean decision)
    {
        this.currentlyElectingNewPrimary = decision;
    }
}
