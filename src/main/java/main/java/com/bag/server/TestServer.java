package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.util.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Class handling the server.
 */
public class TestServer extends DefaultRecoverable
{
    /**
     * Contains the local server replica.
     */
    private ServiceReplica replica = null;

    /**
     * A String describing the database instance.
     */
    private final String instance;

    /**
     * The database instance. todo generify.
     */
    private Neo4jDatabaseAccess neo4j;

    /**
     * Contains all local transactions being executed on the server at the very moment.
     */
    private HashMap<Integer, TransactionStorage> localTransactionList;

    /**
     * Global snapshot id, increases with every committed transaction.
     */
    private long globalSnapshotId = 0;

    private KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        // configure kryo instance, customize settings
        return kryo;
    };

    private TestServer(int id)
    {
        globalSnapshotId = 1;
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        this.replica = new ServiceReplica(id, this, this);
        this.replica.setReplyController(new DefaultReplier());

        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        pool.release(kryo);

        instance = Constants.NEO4J;

        neo4j = new Neo4jDatabaseAccess();

        //todo create terminate command.
        //neo4j.terminate();
    }

    @Override
    public void installSnapshot(final byte[] bytes)
    {

    }

    @Override
    public byte[] getSnapshot()
    {
        return new byte[0];
    }

    //Every byte array is one request.
    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
        //todo when we execute the sets on commit, we have to be careful.
        //todo Follow the following order: First createSet then writeSet and then deleteSet.
        //todo deserialze the hashmaps
        // HashMap<NodeStorage, NodeStorage> deserialized = (HashMap<NodeStorage, NodeStorage>) kryo.readClassAndObject(input);
        return new byte[0][];
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message");
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);
        String reason = kryo.readObject(input, String.class);

        Output output = new Output(0, 10240);

        if(reason.equals(Constants.READ_MESSAGE))
        {
            output = handleNodeRead(input, messageContext, kryo, output);
        }
        else if(reason.equals(Constants.RELATIONSHIP_READ_MESSAGE))
        {
            long localSnapshotId = kryo.readObject(input, Long.class);
            //todo derialize the relationShipStorage object.
            input.close();

            if (localSnapshotId == -1)
            {
                //todo add transaction to localTransactionList
                localSnapshotId = globalSnapshotId;
            }

            if (instance.equals(Constants.NEO4J))
            {
                //todo return result from neo4j + serialize.
            }
        }
        else
        {
            Log.getLogger().warn("Incorrect operation sent unordered to the server");
            output.close();
            input.close();
            return new byte[0];
        }

        byte[] returnValue = output.toBytes();

        Log.getLogger().info("Return it to client, size: " + returnValue.length);

        output.close();
        pool.release(kryo);
        
        return returnValue;
    }

    /**
     * Handles the node read message and requests it to the database.
     * @param input get info from.
     * @param messageContext additional context.
     * @param kryo kryo object.
     * @param output write info to.
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
            localTransactionList.put(messageContext.getSender(), new TransactionStorage());
            localSnapshotId = globalSnapshotId;
        }
        ArrayList<Object> returnList = null;

        if (instance.equals(Constants.NEO4J))
        {
            Log.getLogger().info("Get info from neo4j");
            returnList = new ArrayList<>(neo4j.readObject(identifier, localSnapshotId));
            Log.getLogger().info("Got info from neo4j: " + returnList.size());
        }

        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeClassAndObject(output, Collections.emptyList());
            kryo.writeClassAndObject(output, Collections.emptyList());
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
     * Main method used to start each TestServer.
     * @param args the id for each testServer, set it in the program arguments.
     */
    public static void main(String [] args)
    {
        int serverId = 0;
        for(String arg: args)
        {
            serverId = Integer.parseInt(arg);
        }
        new TestServer(serverId);
    }
}
