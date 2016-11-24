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
import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.server.database.SparkseeDatabaseAccess;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.server.database.OrientDBDatabaseAccess;
import main.java.com.bag.server.database.TitanDatabaseAccess;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.*;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import main.java.com.bag.util.storage.TransactionStorage;

import java.util.*;

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
     * The database instance.
     */
    private IDatabaseAccess databaseAccess;

    //todo maybe detect local transaction problems in the future.
    /**
     * Contains all local transactions being executed on the server at the very moment.
     */
    private HashMap<Integer, TransactionStorage> localTransactionList = new HashMap<>();

    /**
     * Global snapshot id, increases with every committed transaction.
     */
    private long globalSnapshotId = 0;

    /**
     * Write set of the nodes contains updates and deletes.
     */
    private HashMap<Long, List<Operation>> globalWriteSet;

    private KryoFactory factory = () ->
    {
        Kryo kryo = new Kryo();
        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        kryo.register(CreateOperation.class, 250);
        kryo.register(DeleteOperation.class, 300);
        kryo.register(UpdateOperation.class, 350);
        // configure kryo instance, customize settings
        return kryo;
    };

    private TestServer(int id, String instance)
    {
        globalSnapshotId = 1;
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();

        this.replica = new ServiceReplica(id, this, this);
        this.replica.setReplyController(new DefaultReplier());

        kryo.register(NodeStorage.class, 100);
        kryo.register(RelationshipStorage.class, 200);
        pool.release(kryo);

        globalWriteSet = new HashMap<>();

        switch (instance)
        {
            case Constants.NEO4J:
                databaseAccess = new Neo4jDatabaseAccess(id);
                break;
            case Constants.TITAN:
                databaseAccess = new TitanDatabaseAccess(id);
                break;
            case Constants.SPARKSEE:
                databaseAccess = new SparkseeDatabaseAccess(id);
                break;
            case Constants.ORIENTDB:
                databaseAccess = new OrientDBDatabaseAccess(id);
                break;
            default:
                Log.getLogger().warn("Invalid databaseAccess");
        }

        databaseAccess.start();
    }

    @Override
    public void installSnapshot(final byte[] bytes)
    {
        //todo synch all data of the server here from the byte.
    }

    @Override
    public byte[] getSnapshot()
    {
        //todo get all data from the server here
        return new byte[0];
    }

    //Every byte array is one request.
    @Override
    public byte[][] appExecuteBatch(final byte[][] bytes, final MessageContext[] messageContexts)
    {
        for(int i = 0; i < bytes.length; ++i)
        {
            if(messageContexts != null && messageContexts[i] != null)
            {
                KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
                Kryo kryo = pool.borrow();
                Input input = new Input(bytes[i]);

                String type = kryo.readObject(input, String.class);

                if(Constants.COMMIT_MESSAGE.equals(type))
                {
                    return executeCommit(kryo, input);
                }
            }
        }
        return new byte[0][];
    }


    public byte[][] executeCommit(Kryo kryo, Input input)
    {
        Long timeStamp = kryo.readObject(input, Long.class);

        //Read the inputStream.
        Object readsSetNodeX = kryo.readClassAndObject(input);
        Object readsSetRelationshipX = kryo.readClassAndObject(input);
        Object writeSetX = kryo.readClassAndObject(input);

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
        output.writeString(Constants.COMMIT_RESPONSE);

        if (!ConflictHandler.checkForConflict(this.globalWriteSet, localWriteSet, readSetNode, readsSetRelationship, timeStamp, databaseAccess))
        {
            Log.getLogger().info("Found conflict, returning abort");
            output.writeString(Constants.ABORT);
            //Send abort to client and abort
            byte[][] returnBytes = {output.toBytes()};
            output.close();
            return returnBytes;
        }

        //Execute the transaction.
        for(Operation op: localWriteSet)
        {
            op.apply(databaseAccess, globalSnapshotId);
        }

        //Store the write set.
        this.globalWriteSet.put(globalSnapshotId, localWriteSet);

        output.writeString(Constants.COMMIT);
        byte[][] returnBytes = {output.toBytes()};
        output.close();
        Log.getLogger().info("No conflict found, returning commit");
        return returnBytes;
    }

    @Override
    public byte[] appExecuteUnordered(final byte[] bytes, final MessageContext messageContext)
    {
        Log.getLogger().info("Received unordered message");
        KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
        Kryo kryo = pool.borrow();
        Input input = new Input(bytes);
        String reason = kryo.readObject(input, String.class);

        Output output = new Output(0, 100240);

        switch (reason)
        {
            case Constants.READ_MESSAGE:
                output = handleNodeRead(input, messageContext, kryo, output);
                break;
            case Constants.RELATIONSHIP_READ_MESSAGE:
                output = handleRelationshipRead(input, messageContext, kryo, output);
                break;
            default:
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
     * Handles the relationship read message and requests it to the database.
     * @param input get info from.
     * @param messageContext additional context.
     * @param kryo kryo object.
     * @param output write info to.
     * @return output object to return to client.
     */
    private Output handleRelationshipRead(final Input input, final MessageContext messageContext, final Kryo kryo, final Output output)
    {
        long localSnapshotId = kryo.readObject(input, Long.class);
        RelationshipStorage identifier = (RelationshipStorage) kryo.readClassAndObject(input);
        input.close();

        Log.getLogger().info("With snapShot id: " + localSnapshotId);
        if (localSnapshotId == -1)
        {
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetRelationship(identifier);
            localTransactionList.put(messageContext.getSender(), transaction);
            localSnapshotId = globalSnapshotId;
        }
        ArrayList<Object> returnList = null;

        if (databaseAccess instanceof Neo4jDatabaseAccess)
        {
            Log.getLogger().info("Get info from databaseAccess");
            try
            {
                returnList = new ArrayList<>(((Neo4jDatabaseAccess) databaseAccess).readObject(identifier, localSnapshotId));
            }
            catch (OutDatedDataException e)
            {
                Log.getLogger().info("Transaction found conflict - terminating", e);
                terminate();
            }
            Log.getLogger().info("Got info from databaseAccess: " + returnList.size());
        }

        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeClassAndObject(output, new ArrayList<NodeStorage>());
            kryo.writeClassAndObject(output, new ArrayList<RelationshipStorage>());
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
            TransactionStorage transaction = new TransactionStorage();
            transaction.addReadSetNodes(identifier);
            localTransactionList.put(messageContext.getSender(), transaction);
            localSnapshotId = globalSnapshotId;
        }
        ArrayList<Object> returnList = null;


        Log.getLogger().info("Get info from databaseAccess");
        try
        {
            returnList = new ArrayList<>( databaseAccess.readObject(identifier, localSnapshotId));
        }
        catch (OutDatedDataException e)
        {
            Log.getLogger().info("Transaction found conflict - terminating", e);
            terminate();
        }

        if(returnList != null)
        {
            Log.getLogger().info("Got info from databaseAccess: " + returnList.size());
        }


        kryo.writeObject(output, localSnapshotId);

        if (returnList == null || returnList.isEmpty())
        {
            kryo.writeClassAndObject(output, new ArrayList<NodeStorage>());
            kryo.writeClassAndObject(output, new ArrayList<RelationshipStorage>());
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
     * Shuts down the Server.
     */
    private void terminate()
    {
        this.databaseAccess.terminate();
        this.replica.kill();
    }

    /**
     * Main method used to start each TestServer.
     * @param args the id for each testServer, set it in the program arguments.
     */
    public static void main(String [] args)
    {
        int serverId = 0;
        String instance = Constants.NEO4J;

        if(args.length == 1)
        {
            try
            {
                serverId = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException ne)
            {
                Log.getLogger().warn("Invalid program arguments, terminating server");
                return;
            }
        }
        else if(args.length == 2)
        {
            try
            {
                serverId = Integer.parseInt(args[0]);
            }
            catch (NumberFormatException ne)
            {
                Log.getLogger().warn("Invalid program arguments, terminating server");
                return;
            }

            String tempInstance = args[1];

            if(tempInstance.toLowerCase().contains("titan"))
            {
                instance = Constants.TITAN;
            }
            else if(tempInstance.toLowerCase().contains("orientdb"))
            {
                instance = Constants.ORIENTDB;
            }
            else if(tempInstance.toLowerCase().contains("sparksee"))
            {
                instance = Constants.SPARKSEE;
            }
            else
            {
                instance = Constants.NEO4J;
            }
        }


        TestServer server = new TestServer(serverId, instance);
        
        Scanner reader = new Scanner(System.in);  // Reading from System.in
        Log.getLogger().info("Write anything to the console to kill this process");
        String command = reader.next();

        if(command != null)
        {
            server.terminate();
        }

    }
}
