package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.operations.IOperation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class handling server communication in the global cluster.
 */
public class GlobalClusterSlave extends AbstractRecoverable
{
    /**
     * Next proxy to deliver messages to.
     */
    private final ServiceProxy localProxy;

    /**
     * The place the local config file lays. This + the cluster id will contain the concrete cluster config location.
     */
    private static final String LOCAL_CONFIG_LOCATION = "local%d/config";

    /**
     * Name of the location of the global config.
     */
    private static final String GLOBAL_CONFIG_LOCATION = "global/config";

    /**
     * The wrapper class instance. Used to access the global cluster if possible.
     */
    private final ServerWrapper wrapper;

    /**
     * Thread pool for message sending.
     */
    private final ExecutorService service = Executors.newSingleThreadExecutor();

    /**
     * The id of the local cluster.
     */
    private final int id;

    GlobalClusterSlave(final int id, @NotNull final ServerWrapper wrapper, final ServerInstrumentation instrumentation)
    {
        super(id, GLOBAL_CONFIG_LOCATION, wrapper, instrumentation);
        this.id = id;
        this.wrapper = wrapper;

        int sendToId = id + 1;
        if(sendToId >= super.getReplica().getReplicaContext().getCurrentView().getN())
        {
            sendToId = 0;
        }
        localProxy = new ServiceProxy(1100 + this.id, String.format(LOCAL_CONFIG_LOCATION, sendToId));
    }

    /**
     * Every byte array is one request.
     *
     * @param bytes           the requests.
     * @param messageContexts the contexts.
     * @return the answers of all requests in this batch.
     */
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
        ArrayList<NodeStorage> readSetNode;
        ArrayList<RelationshipStorage> readsSetRelationship;
        ArrayList<IOperation> localWriteSet;

        input.close();
        Output output = new Output(0, 2056);
        kryo.writeObject(output, Constants.COMMIT_RESPONSE);

        try
        {
            readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
            readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            localWriteSet = (ArrayList<IOperation>) writeSetX;
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

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(),
                super.getLatestWritesSet(),
                new ArrayList<>(localWriteSet),
                readSetNode,
                readsSetRelationship,
                timeStamp,
                wrapper.getDataBaseAccess()))
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
                //Log.getLogger().info("Global: " + super.getGlobalWriteSet().size() + " id: " + super.getId());
                //Log.getLogger().info("Latest: " + super.getLatestWritesSet().size() + " id: " + super.getId());
            }

            //Send abort to client and abort
            byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }
        final long localSnapshotId = getGlobalSnapshotId();

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, localSnapshotId);

        if (!localWriteSet.isEmpty())
        {
            Log.getLogger().info("Comitting: " + localSnapshotId + " localId: " + timeStamp);
            //Log.getLogger().info("Global: " + super.getGlobalWriteSet().size() + " id: " + super.getId());
            //Log.getLogger().info("Latest: " + super.getLatestWritesSet().size() + " id: " + super.getId());

            super.executeCommit(localWriteSet);
            if (wrapper.getLocalCLuster() != null)
            {
                final Output slaveUpdateOutput = new Output(0,2056);
                kryo.writeObject(slaveUpdateOutput, Constants.UPDATE_SLAVE);
                kryo.writeObject(slaveUpdateOutput, localSnapshotId);
                kryo.writeObject(slaveUpdateOutput, localWriteSet);

                final MessageThread runnable = new MessageThread(slaveUpdateOutput.getBuffer());
                //updateSlave(slaveUpdateOutput.getBuffer());
                //updateNextSlave(slaveUpdateOutput.getBuffer());
                service.execute(runnable);
                slaveUpdateOutput.close();
            }
        }
        else
        {
            updateCounts(0, 0, 1, 0);
        }

        byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
    }

    private class MessageThread implements Runnable
    {
        private final byte[] message;
        MessageThread(byte[] message)
        {
            this.message = message;
        }

        @Override
        public void run()
        {
            updateSlave(message);
            updateNextSlave(message);
        }

        private void updateNextSlave(final byte[] buffer)
        {
            Log.getLogger().info("Notifying next cluster: " + localProxy.getProcessId() + " processes: " + localProxy.getViewManager().getCurrentViewProcesses().length);
            localProxy.sendMessageToTargets(buffer, 0 , localProxy.getViewManager().getCurrentViewProcesses(), TOMMessageType.ORDERED_REQUEST);
        }

        /**
         * Update the slave with a transaction.
         *
         * @param message the message to propagate.
         */
        private void updateSlave(final byte[] message)
        {
            if (wrapper.getLocalCLuster() != null)
            {
                Log.getLogger().info("Notifying local cluster!");
                wrapper.getLocalCLuster().propagateUpdate(message);
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
    private byte[] executeReadOnlyCommit(final Kryo kryo, final Input input, final long timeStamp)
    {
        //Read the inputStream.
        final List readsSetNodeX = kryo.readObject(input, ArrayList.class);
        final List readsSetRelationshipX = kryo.readObject(input, ArrayList.class);
        final List writeSetX = kryo.readObject(input, ArrayList.class);

        //Create placeHolders.
        ArrayList<NodeStorage> readSetNode;
        ArrayList<RelationshipStorage> readsSetRelationship;
        ArrayList<IOperation> localWriteSet;

        input.close();
        Output output = new Output(128);
        kryo.writeObject(output, Constants.COMMIT_RESPONSE);

        try
        {
            readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
            readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            localWriteSet = (ArrayList<IOperation>) writeSetX;
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

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(),
                super.getLatestWritesSet(),
                localWriteSet,
                readSetNode,
                readsSetRelationship,
                timeStamp,
                wrapper.getDataBaseAccess()))
        {
            updateCounts(0, 0, 0, 1);

            Log.getLogger()
                    .info("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: "
                            + localWriteSet.size()
                            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            byte[] returnBytes = output.getBuffer();
            output.close();
            return returnBytes;
        }

        updateCounts(0, 0, 1, 0);

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        byte[] returnBytes = output.getBuffer();
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
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
                    output = handleNodeRead(input, kryo, output);
                }
                catch (Exception t)
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
                    output = handleRelationshipRead(input, kryo, output);
                }
                catch (Exception t)
                {
                    Log.getLogger().error("Error on " + Constants.RELATIONSHIP_READ_MESSAGE + ", returning empty read", t);
                    output = makeEmptyReadResponse(Constants.RELATIONSHIP_READ_MESSAGE, kryo);
                }
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
        return executeReadOnlyCommit(kryo, input, timeStamp);
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
    @SuppressWarnings("squid:S2095")
    private byte[] handleRegisteringSlave(final Input input, final Kryo kryo)
    {
        final int localClusterID = kryo.readObject(input, Integer.class);
        final int newPrimary = kryo.readObject(input, Integer.class);
        final int oldPrimary = kryo.readObject(input, Integer.class);

        final ServiceProxy tempProxy = new ServiceProxy(1000 + oldPrimary, "local" + localClusterID);

        final Output output = new Output(512);

        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);
        kryo.writeObject(output, newPrimary);

        byte[] result = tempProxy.invokeUnordered(output.getBuffer());


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
        tempProxy.close();
        output.close();
        return returnBuffer;
        //remove currentView and edit system.config
        //If alright send the result to all remaining global clusters so that they update themselves.
    }

    @Override
    protected void readSpecificData(final Input input, final Kryo kryo)
    {
        /*
         * We don't have anything to store in here right now.
         */
    }

    @Override
    protected final Output writeSpecificData(final Output output, final Kryo kryo)
    {
        return new Output(0);
    }

    @Override
    public void putIntoWriteSet(final long currentSnapshot, final List<IOperation> localWriteSet)
    {
        super.putIntoWriteSet(currentSnapshot, localWriteSet);
        if (wrapper.getLocalCLuster() != null)
        {
            wrapper.getLocalCLuster().putIntoWriteSet(currentSnapshot, localWriteSet);
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
        return new Output();
    }

    /**
     * Closes the global cluster and his code.
     */
    public void close()
    {
        super.terminate();
    }
}
