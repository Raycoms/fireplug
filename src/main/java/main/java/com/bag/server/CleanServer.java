package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

/**
 * Server used to communicate with graph databases directly without the use of BAG.
 */
public class CleanServer
{
    /**
     * Create a threadsafe version of kryo.
     */
    private KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        return kryo;
    };

    /**
     * Main method to start the clean server.
     * @param args the arguments to start it with.
     */
    public static void main(String[] args)
    {

    }
}
