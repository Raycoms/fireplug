package main.java.com.bag.server;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceProxy;
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

import java.io.*;
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
     * Used to convert nano time to seconds.
     */
    private static final double NANOTIMEDIVIDER = 1000000000.0;

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
     * Amount of data sent since last reset.
     */
    private int throughput;

    /**
     * Amount of aborts since last reset.
     */
    private int aborts;

    /**
     * Amount of commits since last reset.
     */
    private int committedTransactions;

    /**
     * Time of last commit.
     */
    private double lastCommit;

    /**
     * The serviceProxy to establish communication with the other replicas.
     */
    private final ServiceProxy proxy;


    public GlobalClusterSlave(final int id, @NotNull final ServerWrapper wrapper)
    {
        super(id, GLOBAL_CONFIG_LOCATION, wrapper);
        this.id = id;
        this.wrapper = wrapper;
        this.proxy = new ServiceProxy(id , GLOBAL_CONFIG_LOCATION);
        throughput = 0;
        aborts = 0;
        committedTransactions = 0;
        lastCommit = System.nanoTime()/NANOTIMEDIVIDER;

        try(final FileWriter file = new FileWriter(System.getProperty("user.home") + "/results"+id+".txt", true);
            final BufferedWriter bw = new BufferedWriter(file);
            final PrintWriter out = new PrintWriter(bw))
        {
            out.println();
            out.println("Starting new experiment: ");
            out.println();
        }
        catch (IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
    }

    //Every byte array is one request.
    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
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

                    /*//Check if we have the transaction id already in the writeSet. If so no need to do this here!
                    if(getGlobalWriteSet().containsKey(timeStamp))
                    {
                        Log.getLogger().info("Already committed that timestamp!");
                        continue;
                    }*/
                    return executeCommit(kryo, input, timeStamp, i);
                }
            }
        }
        Log.getLogger().info("Return empty bytes.");
        return new byte[0][];
    }

    @Override
    void readSpecificData(final Input input, final Kryo kryo)
    {
        final int length = kryo.readObject(input, Integer.class);
        for(int i = 0; i < length; i++)
        {
            try
            {
                signatureStorageMap.put(kryo.readObject(input, Long.class), kryo.readObject(input, SignatureStorage.class));
            }
            catch (ClassCastException ex)
            {
                Log.getLogger().info("Unable to restore signatureStoreMap entry: " + i + " at server: " + id, ex);
            }
        }
    }

    @Override
    void writeSpecificData(final Output output, final Kryo kryo)
    {
        if(signatureStorageMap != null)
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
     * @param kryo the kryo instance.
     * @param input the input.
     * @return the response.
     */
    private byte[][] executeCommit(final Kryo kryo, final Input input, final long timeStamp, final int i)
    {
        //Read the inputStream.
        final List readsSetNodeX = kryo.readObject(input, ArrayList.class);
        final List readsSetRelationshipX = kryo.readObject(input, ArrayList.class);
        final List writeSetX = kryo.readObject(input, ArrayList.class);

        //Create placeHolders.
        ArrayList<NodeStorage> readSetNode;
        ArrayList<RelationshipStorage> readsSetRelationship;
        ArrayList<Operation> localWriteSet;

        try
        {
            readSetNode = (ArrayList<NodeStorage>) readsSetNodeX;
            readsSetRelationship = (ArrayList<RelationshipStorage>) readsSetRelationshipX;
            localWriteSet = (ArrayList<Operation>) writeSetX;
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Couldn't convert received data to sets. Returning abort", e);
            return new byte[0][];
        }

        input.close();
        Output output = new Output(1024);
        kryo.writeObject(output, Constants.COMMIT_RESPONSE);

        boolean printResult = false;
        if(System.nanoTime()/NANOTIMEDIVIDER - lastCommit >= 30)
        {
            lastCommit = System.nanoTime()/NANOTIMEDIVIDER;
            printResult = true;
        }

        if (!ConflictHandler.checkForConflict(super.getGlobalWriteSet(), localWriteSet, readSetNode, readsSetRelationship, timeStamp, wrapper.getDataBaseAccess()))
        {
            aborts+=1;

            if(printResult)
            {
                writeToFile(aborts, committedTransactions, throughput, lastCommit);
            }

            Log.getLogger().info("Found conflict, returning abort with timestamp: " + timeStamp + " globalSnapshot at: " + getGlobalSnapshotId() + " and writes: " + localWriteSet.size()
            + " and reads: " + readSetNode.size() + " + " + readsSetRelationship.size());
            kryo.writeObject(output, Constants.ABORT);
            kryo.writeObject(output, getGlobalSnapshotId());

            //Send abort to client and abort
            byte[][] returnBytes = {output.getBuffer()};
            output.close();

            if(wrapper.getLocalCLuster() != null)
            {
                signCommitWithDecisionAndDistribute(localWriteSet, Constants.ABORT, -1, kryo);
            }
            return returnBytes;
        }

        throughput+=localWriteSet.size();
        committedTransactions+=1;

        if(printResult)
        {
            writeToFile(aborts, committedTransactions, throughput, lastCommit);
            aborts = 0;
            committedTransactions = 0;
            throughput = 0;
        }

        final long snapShotId = super.executeCommit(localWriteSet);
        if(wrapper.getLocalCLuster() != null)
        {
            signCommitWithDecisionAndDistribute(localWriteSet, Constants.COMMIT, snapShotId, kryo);
        }

        kryo.writeObject(output, Constants.COMMIT);
        kryo.writeObject(output, getGlobalSnapshotId());

        byte[][] returnBytes = {output.getBuffer()};
        output.close();
        Log.getLogger().info("No conflict found, returning commit with snapShot id: " + getGlobalSnapshotId() + " size: " + returnBytes.length);

        return returnBytes;
    }

    private void writeToFile(final int aborts, final int commits, final int throughput, final double time)
    {
        try(final FileWriter file = new FileWriter("/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/config/results"+id+".txt", true);
            final BufferedWriter bw = new BufferedWriter(file);
            final PrintWriter out = new PrintWriter(bw))
        {
            out.println(time + ", ");
            out.print(aborts + ", ");
            out.print(commits + ", ");
            out.print(String.valueOf(throughput));
            out.println();
        }
        catch (IOException e)
        {
            Log.getLogger().info("Problem while writing to file!", e);
        }
    }

    private void signCommitWithDecisionAndDistribute(final List<Operation> localWriteSet, final String decision, final long snapShotId, final Kryo kryo)
    {
        final RSAKeyLoader rsaLoader = new RSAKeyLoader(this.id, GLOBAL_CONFIG_LOCATION, false);

        //Todo probably will need a bigger buffer in the future. size depending on the set size?
        Output output = new Output(0, 100240);

        kryo.writeObject(output, Constants.SIGNATURE_MESSAGE);
        kryo.writeObject(output, decision);

        //Write the timeStamp to the server
        kryo.writeObject(output, snapShotId);

        kryo.writeObject(output, localWriteSet);

        byte[] bytes;

        try
        {
            bytes = TOMUtil.signMessage(rsaLoader.loadPrivateKey(), output.getBuffer());
        }
        catch (Exception e)
        {
            Log.getLogger().warn("Unable to sign message at server " + id, e);
            return;
        }

        final SignatureStorage signatureStorage = new SignatureStorage(super.getReplica().getReplicaContext().getStaticConfiguration().getN(), output.getBuffer(), decision);
        signatureStorage.addSignatures(id, bytes);
        signatureStorageMap.put(snapShotId, signatureStorage);

        kryo.writeObject(output, bytes.length);
        kryo.writeObject(output, bytes);

        proxy.invokeUnordered(output.getBuffer());
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message at global replica");
        KryoPool pool = new KryoPool.Builder(getFactory()).softReferences().build();
        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);

        final String messageType = kryo.readObject(input, String.class);
        Output output = new Output(0, 100240);

        switch(messageType)
        {
            case Constants.READ_MESSAGE:
                Log.getLogger().info("Received Node read message");
                kryo.writeObject(output, Constants.READ_MESSAGE);
                output = handleNodeRead(input, messageContext, kryo, output);
                break;
            case Constants.RELATIONSHIP_READ_MESSAGE:
                Log.getLogger().info("Received Relationship read message");
                kryo.writeObject(output, Constants.READ_MESSAGE);
                output = handleRelationshipRead(input, messageContext, kryo, output);
                break;
            case Constants.SIGNATURE_MESSAGE:
                handleSignatureMessage(input, messageContext, kryo);
                break;
            case Constants.REGISTER_GLOBALLY_MESSAGE:
                output.close();
                return handleRegisteringSlave(input, kryo);
            case Constants.REGISTER_GLOBALLY_CHECK:
                output.close();
                return handleGlobalRegistryCheck(input, kryo);
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

    /**
     * Handle the check for the global registering of a slave.
     * @param input the incoming message.
     * @param kryo the kryo instance.
     * @return the reply
     */
    private byte[] handleGlobalRegistryCheck(final Input input, final Kryo kryo)
    {
        final Output output = new Output(512);
        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);

        boolean decision = false;
        if(!wrapper.getLocalCLuster().isPrimary() || wrapper.getLocalCLuster().askIfIsPrimary(kryo.readObject(input, Integer.class), kryo.readObject(input, Integer.class), kryo))
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
     * @param kryo the kryo instance.
     * @param input the message.
     * @return the message in bytes.
     */
    private byte[] handleRegisteringSlave(final Input input, final Kryo kryo)
    {
        final int localClusterID = kryo.readObject(input, Integer.class);
        final int newPrimary = kryo.readObject(input, Integer.class);
        final int oldPrimary = kryo.readObject(input, Integer.class);

        final ServiceProxy localProxy = new ServiceProxy(oldPrimary, "local" + localClusterID);

        final Output output = new Output(512);

        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_CHECK);
        kryo.writeObject(output, newPrimary);

        byte[] result = localProxy.invokeUnordered(output.getBuffer());


        final Output nextOutput = new Output(512);
        kryo.writeObject(output, Constants.REGISTER_GLOBALLY_REPLY);

        final Input answer = new Input(result);
        if(Constants.REGISTER_GLOBALLY_REPLY.equals(answer.readString()))
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
     * Handle a signature message.
     * @param input the message.
     * @param messageContext the context.
     * @param kryo the kryo object.
     */
    private void handleSignatureMessage(final Input input, final MessageContext messageContext, final Kryo kryo)
    {
        final String decision = kryo.readObject(input, String.class);;
        final Long snapShotId = kryo.readObject(input, Long.class);;
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

        Log.getLogger().info("Server: " + id + "Received message to sign with snapShotId: "
                + snapShotId + "of Server"
                + messageContext.getSender()
                + " and decision: " + decision
                + " and a writeSet of the length of: " + localWriteSet.size());


        final int signatureLength = kryo.readObject(input, Integer.class);;
        final byte[] signature = input.readBytes(signatureLength);


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

        final byte[] message = new byte[input.getBuffer().length - signatureLength];
        System.arraycopy(message, 0, input.getBuffer(), 0, input.getBuffer().length - signatureLength);

        boolean signatureMatches = TOMUtil.verifySignature(key, message, signature);

        if(signatureMatches)
        {
            storeSignedMessage(snapShotId, signature, messageContext, decision);
        }

        input.close();
    }

    /**
     * Store the signed message on the server.
     * If n-f messages arrived send it to client.
     * @param snapShotId the snapShotId as key.
     * @param signature the signature
     * @param context the message context.
     * @param decision the decision.
     */
    private void storeSignedMessage(final Long snapShotId, final byte[] signature, @NotNull final MessageContext context, final String decision)
    {
        final SignatureStorage signatureStorage;
        if(!signatureStorageMap.containsKey(snapShotId))
        {
            Log.getLogger().warn("Replica: " + id + " did not have the transaction prepared. Might be slow or corrupted.");
            return;
        }

        signatureStorage = signatureStorageMap.get(snapShotId);

        if(!decision.equals(signatureStorage.getDecision()))
        {
            Log.getLogger().warn("Replica: " + id + " did receive a different decision of replica: " + context.getSender() + ". Might be corrupted.");
            return;
        }
        signatureStorage.addSignatures(context.getSender(), signature);

        if(signatureStorage.hasEnough())
        {
            updateSlave(signatureStorage);
            signatureStorageMap.remove(snapShotId);
            return;
        }
        signatureStorageMap.put(snapShotId, signatureStorage);
    }

    /**
     * Update the slave with a transaction.
     * @param signatureStorage the signatureStorage with message and signatures..
     */
    private void updateSlave(final SignatureStorage signatureStorage)
    {
        this.wrapper.getLocalCLuster().propagateUpdate(signatureStorage);
    }

    /**
     * Invoke a message to the global cluster.
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
