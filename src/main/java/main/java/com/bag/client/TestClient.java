package main.java.com.bag.client;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.util.Extractor;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import main.java.com.bag.util.NodeStorage;
import main.java.com.bag.util.RelationshipStorage;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Class handling the client.
 */
public class TestClient extends ServiceProxy
{
    /**
     * Sets to log reads, updates, deletes and node creations.
     */
    private HashMap<NodeStorage, NodeStorage> readsSetNode;
    private HashMap<NodeStorage, NodeStorage> updateSetNode;
    private ArrayList<NodeStorage>            deleteSetNode;
    private ArrayList<NodeStorage>            createSetNode;

    /**
     * Sets to log reads, updates, deletes and relationship creations.
     */
    private HashMap<RelationshipStorage, RelationshipStorage> readsSetRelationship;
    private HashMap<RelationshipStorage, RelationshipStorage> updateSetRelationship;
    private ArrayList<RelationshipStorage>                    deleteSetRelationship;
    private ArrayList<RelationshipStorage>                    createSetRelationship;

    /**
     * Local timestamp of the current transaction.
     */
    private int localTimeStamp = 0;

    /**
     * Kyro object.
     */
    private Kryo kryo = new Kryo();
    MapSerializer        mapSerializer  = new MapSerializer();
    CollectionSerializer listSerializer = new CollectionSerializer();


    public TestClient(final int processId)
    {
        super(processId);
        initClient();
    }

    public TestClient(final int processId, final String configHome)
    {
        super(processId, configHome);
        initClient();
    }

    public TestClient(final int processId, final String configHome, final Comparator<byte[]> replyComparator, final Extractor replyExtractor)
    {
        super(processId, configHome, replyComparator, replyExtractor);
        initClient();
    }

    /**
     * Initiates the client maps and registers necessary operations.
     */
    private void initClient()
    {
        kryo.register(NodeStorage.class, 1);
        kryo.register(RelationshipStorage.class, 2);
        kryo.register(HashMap.class, mapSerializer);

        mapSerializer.setKeyClass(NodeStorage.class, kryo.getSerializer(NodeStorage.class));
        mapSerializer.setKeyClass(RelationshipStorage.class, kryo.getSerializer(RelationshipStorage.class));
        mapSerializer.setValuesCanBeNull(false);
        mapSerializer.setKeysCanBeNull(false);
        listSerializer.setElementClass(NodeStorage.class, kryo.getSerializer(NodeStorage.class));
        listSerializer.setElementClass(RelationshipStorage.class, kryo.getSerializer(RelationshipStorage.class));
        listSerializer.setElementsCanBeNull(false);

        readsSetNode = new HashMap<NodeStorage, NodeStorage>();
        updateSetNode = new HashMap<NodeStorage, NodeStorage>();
        deleteSetNode = new ArrayList<NodeStorage>();
        createSetNode = new ArrayList<NodeStorage>();

        readsSetRelationship = new HashMap<RelationshipStorage, RelationshipStorage>();
        updateSetRelationship = new HashMap<RelationshipStorage, RelationshipStorage>();
        deleteSetRelationship = new ArrayList<RelationshipStorage>();
        createSetRelationship = new ArrayList<RelationshipStorage>();

        commit();
    }

    /**
     * Write requests. (Only reach database on commit)
     */
    public void write()
    {

    }

    /**
     * ReadRequests.(Directly read database)
     */
    public void read()
    {
        //invokeUnordered();
    }

    /**
     * Commit reaches the server, if secure commit send to all, else only send to one
     */
    public void commit()
    {
        //todo also send local timestamp.

        readsSetNode.put(new NodeStorage("a"), new NodeStorage("e"));
        readsSetNode.put(new NodeStorage("b"), new NodeStorage("f"));
        readsSetNode.put(new NodeStorage("c"), new NodeStorage("g"));
        readsSetNode.put(new NodeStorage("d"), new NodeStorage("h"));


        boolean readOnly = true;
        boolean secureMode = false;

        byte[] bytes = serialize();


        if(readOnly && !secureMode)
        {
            invokeUnordered(bytes);
        }
        else
        {
            invokeOrdered(bytes);
        }


    }


    public void write(final Kryo kryo, final com.esotericsoftware.kryo.io.Output output)
    {

    }

    public byte[] serialize()
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Output output = new Output(stream);
        mapSerializer.write(kryo, output, readsSetNode);
        byte[] bytes = output.toBytes();
        output.close();
        return bytes;

    }




}
