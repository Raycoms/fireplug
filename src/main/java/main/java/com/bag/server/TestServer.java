package main.java.com.bag.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.MapSerializer;
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
    private Kryo kryo = new Kryo();
    MapSerializer serializer = new MapSerializer();


    private TestServer(int id)
    {
        this.replica = new ServiceReplica(id, this, this);
        kryo.register(NodeStorage.class, 1);
        kryo.register(RelationshipStorage.class, 2);
        kryo.register(HashMap.class, serializer);

        serializer.setKeyClass(NodeStorage.class, kryo.getSerializer(NodeStorage.class));
        serializer.setKeyClass(RelationshipStorage.class, kryo.getSerializer(RelationshipStorage.class));
        serializer.setValuesCanBeNull(false);
        serializer.setKeysCanBeNull(false);
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
        Input input = new Input(bytes);
        HashMap<NodeStorage, NodeStorage> deserialized = (HashMap<NodeStorage, NodeStorage>) kryo.readClassAndObject(input);
        input.close();


        return new byte[0];
    }

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
