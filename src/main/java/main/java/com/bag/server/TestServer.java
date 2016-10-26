package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

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

    KryoFactory factory = new KryoFactory() {
        public Kryo create () {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            // configure kryo instance, customize settings
            return kryo;
        }
    };
    private long globalSnapshotId = 0;

    private TestServer(int id)
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        this.replica = new ServiceReplica(id, this, this);
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);

        pool.release(kryo);
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

    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
        return new byte[0][];
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        Input input = new Input(bytes);
        String reason = (String) kryo.readClassAndObject(input);

        if(reason.equals(Constants.NODE_READ_MESSAGE))
        {
            long localSnapshotId = (long) kryo.readClassAndObject(input);
            HashMap<NodeStorage, NodeStorage> deserialized = (HashMap<NodeStorage, NodeStorage>) kryo.readClassAndObject(input);

            if(localSnapshotId == -1)
            {
                //todo add transaction to localTransactionList
                localSnapshotId = globalSnapshotId;
            }

            //todo return result from neo4j + serialize.
        }
        else if(reason.equals(Constants.RELATIONSHIP_READ_MESSAGE))
        {
            long localSnapshotId = (long) kryo.readClassAndObject(input);
            HashMap<RelationshipStorage, RelationshipStorage> deserialized = (HashMap<RelationshipStorage, RelationshipStorage>) kryo.readClassAndObject(input);

            if(localSnapshotId == -1)
            {
                //todo add transaction to localTransactionList
                localSnapshotId = globalSnapshotId;
            }

            //todo return result from neo4j + serialize.
        }
        else if(reason.equals(Constants.COMMIT_MESSAGE))
        {
            //commit probably, shouldn't happen, commit only ordered.
        }
        else
        {
            Log.getLogger().warn("Incorrect operation sent unordered to the server");
            return new byte[0];
        }

        input.close();

        pool.release(kryo);

        return new byte[0];
    }

    //todo when we execute the sets on commit, we have to be careful.
    //First createSet then writeSet and then deleteSet.


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
