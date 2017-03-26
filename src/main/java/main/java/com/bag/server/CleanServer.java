package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.evaluations.ClientThreads;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Server used to communicate with graph databases directly without the use of BAG.
 */
public class CleanServer
{
    /**
     * Create a threadsafe version of kryo.
     */
    private static KryoFactory factory = () ->
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
        if(args.length < 1)
        {
            return;
        }

        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final int serverPort = Integer.parseInt(args[0]);
        try
                (
                        final Socket socket = new Socket("", serverPort);
                        final DataInputStream in = new DataInputStream(socket.getInputStream());
                        BufferedReader console = new BufferedReader(new InputStreamReader(System.in))
                )
        {
            while (! "quit".equals(console.readLine()))
            {

                final Input input = new Input(in);
                if(input.getBuffer().length == 0)
                {
                    continue;
                }

                List returnValue = kryo.readObject(input, ArrayList.class);

                if(!returnValue.isEmpty() && returnValue.get(0) instanceof Operation)
                {
                    Neo4jDatabaseAccess access = new Neo4jDatabaseAccess(0);
                    final List<Operation> operations = returnValue;
                    for (Operation op : operations)
                    {
                        op.apply(access, 0);
                    }
                }
                //todo else it is a read and we should handle the read.
                //todo we will mix reads and operations maybe and run it through a for loop here?

                //todo might do the statistical read here
            }
        }
        catch (IOException ex)
        {
            Log.getLogger().warn("IOException while reading socket", ex);
        }



    }
}
