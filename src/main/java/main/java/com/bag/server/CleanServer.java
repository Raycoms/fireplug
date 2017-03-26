package main.java.com.bag.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.evaluations.ClientThreads;
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Constants;
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
        if(args.length < 3)
        {
            return;
        }

        final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        final Kryo kryo = pool.borrow();


        final int serverPort = Integer.parseInt(args[0]);
        final String address = args[1];
        final String tempInstance = args[2];

        final String instance;

        if (tempInstance.toLowerCase().contains("titan"))
        {
            instance = Constants.TITAN;
        }
        else if (tempInstance.toLowerCase().contains("orientdb"))
        {
            instance = Constants.ORIENTDB;
        }
        else if (tempInstance.toLowerCase().contains("sparksee"))
        {
            instance = Constants.SPARKSEE;
        }
        else
        {
            instance = Constants.NEO4J;
        }

        final IDatabaseAccess access = ServerWrapper.instantiateDBAccess(instance, 0);

        if(access == null)
        {
            return;
        }
        access.start();

        int executedOperations = 0;
        try
                (
                        final ServerSocket socket = new ServerSocket( serverPort);
                        Socket clientSocket = socket.accept();
                        final DataInputStream in = new DataInputStream(clientSocket.getInputStream());
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

                for(Object obj: returnValue)
                {
                    if (obj instanceof Operation)
                    {
                        ((Operation) obj).apply(access, 0);
                        executedOperations++;
                    }
                    else if(obj instanceof NodeStorage)
                    {
                        try
                        {
                            access.readObject((NodeStorage) obj, 0);
                            executedOperations++;
                        }
                        catch (OutDatedDataException e)
                        {
                            Log.getLogger().info("Unable to retrieve data at clean server with instance: " + instance, e);
                        }
                    }
                }
            }
        }
        catch (IOException ex)
        {
            Log.getLogger().warn("IOException while reading socket", ex);
        }

        Log.getLogger().info("Executed operations: " + executedOperations);
        access.terminate();
    }
}
