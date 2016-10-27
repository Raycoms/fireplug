package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.server.Replier;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultReplier;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.util.ArrayList;
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
        neo4j.start(id);

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
        String reason = (String) kryo.readClassAndObject(input);
        Output output = new Output(0, 10240);

        if(reason.equals(Constants.NODE_READ_MESSAGE))
        {
            long localSnapshotId = (long) kryo.readClassAndObject(input);
            //todo deserialize nodeStorage object.
            input.close();

            Log.getLogger().info("With snapShot id: " + localSnapshotId);
            if(localSnapshotId == -1)
            {
                //todo add transaction to localTransactionList
                localSnapshotId = globalSnapshotId;
            }

            if(instance.equals(Constants.NEO4J))
            {
                Log.getLogger().info("Get info from neo4j");

                //todo add some real Nodes from deserialized map.
                ArrayList<NodeStorage> tempList = new ArrayList<>(neo4j.randomRead());

                Log.getLogger().info("Got info from neo4j: " + tempList.size());

                kryo.writeClassAndObject(output, localSnapshotId);
                kryo.writeClassAndObject(output, tempList);
            }
        }
        else if(reason.equals(Constants.RELATIONSHIP_READ_MESSAGE))
        {
            long localSnapshotId = (long) kryo.readClassAndObject(input);
            //todo derialize the relationShipStorage object.
            input.close();

            if(localSnapshotId == -1)
            {
                //todo add transaction to localTransactionList
                localSnapshotId = globalSnapshotId;
            }

            if(instance.equals(Constants.NEO4J))
            {
                //todo return result from neo4j + serialize.
            }
        }
        else
        {
            Log.getLogger().warn("Incorrect operation sent unordered to the server");
            output.close();
            return new byte[0];
        }

        byte[] returnValue = output.toBytes();

        Log.getLogger().info("Return it to client, size: " + returnValue.length);

        output.close();
        pool.release(kryo);
        
        return returnValue;
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
