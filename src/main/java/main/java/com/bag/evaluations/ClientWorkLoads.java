package main.java.com.bag.evaluations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.client.TestClient;
import main.java.com.bag.operations.CreateOperation;
import main.java.com.bag.operations.DeleteOperation;
import main.java.com.bag.operations.Operation;
import main.java.com.bag.operations.UpdateOperation;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Class containing the threads to simulate concurrent clients.
 */
public class ClientWorkLoads
{
    /**
     * Location of the testGraph.
     */
    private static final String GRAPH_LOCATION = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/src/testGraphs/social-a-graph.txt";

    private ClientWorkLoads()
    {
        /*
         * Intentionally left empty.
         */
    }

    public static class MassiveNodeInsertThread
    {
        private TestClient  client = null;
        private NettyClient out    = null;

        private final int        startAt;
        private final int        stopAt;
        private final int        commitAfter;

        /**
         * Create a threadsafe version of kryo.
         */
        public KryoFactory factory = () ->
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            kryo.register(CreateOperation.class, 250);
            kryo.register(DeleteOperation.class, 300);
            kryo.register(UpdateOperation.class, 350);
            return kryo;
        };

        public MassiveNodeInsertThread(@NotNull final TestClient client, final int share, final int start, final int commitAfter, final int size)
        {
            this.client = client;
            startAt = start * (size/share) + 1;
            stopAt = startAt + (size/share) - 1;
            this.commitAfter = commitAfter;
        }

        public MassiveNodeInsertThread(final NettyClient out, final int share, final int start, final int commitAfter, final int size)
        {
            this.out = out;
            startAt = start * (size/share) + 1;
            stopAt = startAt + (size/share) - 1;
            this.commitAfter = commitAfter;
            out.runNetty();
        }

        public void run()
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            List<Operation> createNodeOperationList = new ArrayList<>();

            int written = 0;
            for (int i = startAt; i <= stopAt; i++)
            {
                written++;
                if(client == null)
                {
                    createNodeOperationList.add(new CreateOperation<>(new NodeStorage(Integer.toString(i))));

                    if (written >= commitAfter || i == stopAt)
                    {
                        try(final Output output = new Output(0, 10024))
                        {
                            written = 0;
                            kryo.writeObject(output, createNodeOperationList);
                            out.sendMessage(output.getBuffer());
                        }
                        createNodeOperationList = new ArrayList<>();
                    }
                }
                else
                {
                    client.write(null, new NodeStorage(Integer.toString(i)));
                    if (written >= commitAfter || i == stopAt)
                    {
                        written = 0;
                        client.commit();
                    }
                }
            }
            pool.release(kryo);
        }
    }

    public static class MassiveRelationShipInsertThread
    {
        private TestClient  client = null;
        private NettyClient out    = null;

        private final int commitAfter;
        private final int share;
        private final int start;

        /**
         * Create a threadsafe version of kryo.
         */
        public KryoFactory factory = () ->
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            kryo.register(CreateOperation.class, 250);
            kryo.register(DeleteOperation.class, 300);
            kryo.register(UpdateOperation.class, 350);
            return kryo;
        };

        public MassiveRelationShipInsertThread(@NotNull final TestClient client, final int share, final int start, final int commitAfter)
        {
            this.client = client;
            this.share = share;
            this.start = start;
            this.commitAfter = commitAfter;
        }

        public MassiveRelationShipInsertThread(final NettyClient out, final int share, final int start, final int commitAfter)
        {
            this.out = out;
            this.share = share;
            this.start = start;
            this.commitAfter = commitAfter;
            out.runNetty();
        }

        public void run()
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            List<CreateOperation<RelationshipStorage>> createRelationshipOperations = new ArrayList<>();

            try(FileReader fr = new FileReader(GRAPH_LOCATION); BufferedReader br = new BufferedReader(fr);)
            {
                final long size = br.lines().count();
                final long totalShare = size / share;
                final long startAt = start * totalShare + 1;

                //skip to the required amount
                br.skip(startAt - 1);

                int readLines = 0;
                int writtenLines = 0;
                String sCurrentLine;
                while ((sCurrentLine = br.readLine()) != null)
                {
                    final String[] ids = sCurrentLine.split(" ");

                    if(ids.length != 3)
                    {
                        continue;
                    }

                    readLines++;
                    writtenLines++;

                    if(client == null)
                    {
                        createRelationshipOperations.add(new CreateOperation<>(new RelationshipStorage(ids[1], new NodeStorage(ids[0]), new NodeStorage(ids[2]))));

                        if (readLines >= totalShare)
                        {
                            try (final Output output = new Output(0, 10024))
                            {
                                kryo.writeObject(output, createRelationshipOperations);
                                out.sendMessage(output.getBuffer());
                            }
                            break;
                        }

                        if (writtenLines >= commitAfter)
                        {
                            try (final Output output = new Output(0, 10024))
                            {
                                writtenLines = 0;
                                kryo.writeObject(output, createRelationshipOperations);
                                out.sendMessage(output.getBuffer());
                            }
                            createRelationshipOperations = new ArrayList<>();
                        }
                    }
                    else
                    {
                        client.write(null, new RelationshipStorage(ids[1], new NodeStorage(ids[0]), new NodeStorage(ids[2])));
                        if (readLines >= totalShare)
                        {
                            client.commit();
                            break;
                        }

                        if (writtenLines >= commitAfter)
                        {
                            writtenLines = 0;
                            client.commit();
                        }
                    }
                    Log.getLogger().info(sCurrentLine);
                }
            }
            catch (IOException e)
            {
                Log.getLogger().warn("Error reading file", e);
            }
            finally
            {
                pool.release(kryo);
            }
        }
    }

    public static class MixedReadWrite
    {
        private TestClient  client = null;
        private NettyClient out    = null;

        private final int commitAfter;

        /**
         * Create a threadsafe version of kryo.
         */
        public KryoFactory factory = () ->
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            kryo.register(CreateOperation.class, 250);
            kryo.register(DeleteOperation.class, 300);
            kryo.register(UpdateOperation.class, 350);
            return kryo;
        };

        public MixedReadWrite(@NotNull final TestClient client, final int commitAfter)
        {
            this.client = client;
            this.commitAfter = commitAfter;
        }

        public MixedReadWrite(final NettyClient out, final int commitAfter)
        {
            this.out = out;
            this.commitAfter = commitAfter;
            out.runNetty();
        }

        public void run()
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            final List<Operation> operations = new ArrayList<>();
            final int maxNodeId = 100000;
            final int maxRelationShipId = Constants.RELATIONSHIP_TYPES_LIST.length;

            final Random random = new Random();
            for(int i = 1; i < 100000; i++)
            {
                boolean isRead = (random.nextDouble() * 100 + 1) > 0.2;
                RelationshipStorage readRelationship = null;
                NodeStorage readNodeStorage = null;
                Operation operation = null;

                if(isRead)
                {
                    double randomNum = random.nextDouble() * 100 + 1;
                    if (randomNum <= 15.7)
                    {
                        readRelationship = new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))));
                        //get relationship
                    }
                    else if (randomNum <= 15.7 + 55.4)
                    {
                        readRelationship = new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))),
                                new NodeStorage());
                        //get all relationships of a particular node
                    }
                    else
                    {
                        readNodeStorage = new NodeStorage(String.valueOf(random.nextInt(maxNodeId)));
                        //get node
                    }
                }
                else
                {
                    double randomNum = random.nextDouble() * 100 + 1;
                    if (randomNum <= 52.5)
                    {
                        operation = new CreateOperation<>(new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId)))));
                        //add relationship
                    }
                    else if (randomNum <= 52.5 + 9.2)
                    {
                        operation = new DeleteOperation<>(new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId)))));
                        //delete relationship
                    }
                    else if(randomNum <= 52.5 + 9.2 + 16.5)
                    {
                        operation = new CreateOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId))));
                        //add node
                    }
                    else if(randomNum <= 52.5 + 9.2 + 16.5 + 20.7)
                    {
                        operation = new UpdateOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId))),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId))));
                        //update node
                    }
                    else
                    {
                        operation = new DeleteOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId))));
                        //delete node
                    }
                }

                if (client == null)
                {
                    if(isRead)
                    {
                        //todo read on neo4j here!
                        //todo have to use netty for that.
                    }
                    operations.add(operation);
                    if (i%10 == 0)
                    {
                        final Output output = new Output(0, 10024)
                        kryo.writeObject(output, operations);
                        out.sendMessage(output.getBuffer());
                        operations.clear();
                        output.close();
                    }
                }
                else
                {
                    if(isRead)
                    {
                        if(readNodeStorage != null)
                        {
                            client.read(readNodeStorage);
                            try
                            {
                                client.getReadQueue().take();
                            }
                            catch (InterruptedException e)
                            {
                                e.printStackTrace();
                            }
                        }

                        if(readRelationship != null)
                        {
                            client.read(readRelationship);
                            try
                            {
                                client.getReadQueue().take();
                            }
                            catch (InterruptedException e)
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                    else
                    {

                        if (operation instanceof DeleteOperation)
                        {
                            client.write(((DeleteOperation) operation).getObject(), null);
                        }
                        else if (operation instanceof UpdateOperation)
                        {
                            client.write(((UpdateOperation) operation).getKey(), ((UpdateOperation) operation).getValue());
                        }
                        else if(operation instanceof DeleteOperation)
                        {
                            client.write(null, ((DeleteOperation) operation).getObject());
                        }
                    }

                    if (i%10 == 0)
                    {
                        client.commit();
                    }
                }
            }
        }
    }

}
