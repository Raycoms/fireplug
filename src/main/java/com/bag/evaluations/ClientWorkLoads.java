package com.bag.evaluations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.bag.client.BAGClient;
import com.bag.client.DirectAccessClient;
import com.bag.client.TestClient;
import com.bag.operations.CreateOperation;
import com.bag.operations.DeleteOperation;
import com.bag.operations.IOperation;
import com.bag.operations.UpdateOperation;
import com.bag.reconfiguration.sensors.LoadSensor;
import com.bag.util.Constants;
import com.bag.util.Log;
import com.bag.util.storage.NodeStorage;
import com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * Class containing the threads to simulate concurrent clients.
 */
public class ClientWorkLoads
{
    /**
     * Location of the testGraph.
     */
    private static final String GRAPH_LOCATION = System.getProperty("user.home") + "/testGraphs/social-a-graph.txt";

    /**
     * Random seed for all random functions. To guarantee reproducibility.
     */
    private static final int RANDOM_SEED = 493934998;

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

        private final int startAt;
        private final int stopAt;
        private final int commitAfter;

        /**
         * Create a threadsafe version of kryo.
         */
        private final KryoFactory factory = () ->
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            kryo.register(CreateOperation.class, 250);
            kryo.register(DeleteOperation.class, 300);
            kryo.register(UpdateOperation.class, 350);
            kryo.register(LoadSensor.LoadDesc.class, 400);
            return kryo;
        };

        public MassiveNodeInsertThread(@NotNull final TestClient client, final int share, final int start, final int commitAfter, final int size)
        {
            this.client = client;
            startAt = start * (size / share) + 1;
            stopAt = startAt + (size / share) - 1;
            this.commitAfter = commitAfter;
        }

        public MassiveNodeInsertThread(final NettyClient out, final int share, final int start, final int commitAfter, final int size)
        {
            this.out = out;
            startAt = start * (size / share) + 1;
            stopAt = startAt + (size / share) - 1;
            this.commitAfter = commitAfter;
            out.runNetty();
        }

        public void run()
        {
            final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();
            final Kryo kryo = pool.borrow();
            List<IOperation> createNodeOperationList = new ArrayList<>();

            int written = 0;
            for (int i = startAt; i <= stopAt; i++)
            {
                written++;
                if (client == null)
                {
                    createNodeOperationList.add(new CreateOperation<>(new NodeStorage(Integer.toString(i))));

                    if (written >= commitAfter || i == stopAt)
                    {
                        try (final Output output = new Output(0, 10024))
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
            kryo.register(LoadSensor.LoadDesc.class, 400);
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
            final List<CreateOperation<RelationshipStorage>> createRelationshipOperations = new ArrayList<>();

            try (FileReader fr = new FileReader(GRAPH_LOCATION); Scanner scan = new Scanner(fr);)
            {
                final long size = 239736;
                final long totalShare = size / share;
                final long startAt = (start - 1) * totalShare + 1;

                int readLines = 0;
                int writtenLines = 0;
                String sCurrentLine;

                while (scan.hasNext())
                {
                    if (readLines < startAt)
                    {
                        readLines++;
                        continue;
                    }
                    readLines++;

                    if (readLines > totalShare)
                    {
                        return;
                    }

                    sCurrentLine = scan.nextLine();
                    final String[] ids = sCurrentLine.split(" ");

                    if (ids.length != 3)
                    {
                        continue;
                    }

                    writtenLines++;
                    if (client == null)
                    {
                        createRelationshipOperations.add(new CreateOperation<>(new RelationshipStorage(ids[1], new NodeStorage(ids[0]), new NodeStorage(ids[2]))));

                        /*if (readLines >= totalShare)
                        {
                            try (final Output output = new Output(0, 10024))
                            {
                                kryo.writeObject(output, createRelationshipOperations);
                                out.sendMessage(output.getBuffer());
                            }
                            break;
                        }*/

                        if (writtenLines >= commitAfter)
                        {
                            try (final Output output = new Output(0, 10024))
                            {
                                writtenLines = 0;
                                kryo.writeObject(output, createRelationshipOperations);
                                out.sendMessage(output.getBuffer());
                            }
                            createRelationshipOperations.clear();
                        }
                    }
                    else
                    {
                        client.write(null, new RelationshipStorage(ids[1], new NodeStorage(ids[0]), new NodeStorage(ids[2])));
                        /*if (readLines >= totalShare)
                        {
                            client.commit();
                            break;
                        }*/

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
                Log.getLogger().error("Error reading file", e);
            }
            finally
            {
                pool.release(kryo);
            }
        }
    }

    /**
     * Realistic operation workload.
     */
    public static class RealisticOperation
    {
        /**
         * The client running it.
         */
        private final BAGClient client;

        /**
         * The used seed.
         */
        private final int    seed;

        /**
         * The num of operations to commit after.
         */
        private final int    commitAfter;

        /**
         * The percentage of writes.
         */
        private final double percOfWrites;

        /**
         * Count of commits.
         */
        private int commitCount = 0;

        /**
         * Graph Relation class.
         */
        private class GraphRelation
        {
            /**
             * The origin of the relation.
             */
            public String origin;

            /**
             * The destination of the relation.
             */
            public String destination;

            /**
             * The name of it.
             */
            public String relationName;
        }

        /**
         * Create a threadsafe version of kryo.
         */
        public final KryoFactory factory = () ->
        {
            Kryo kryo = new Kryo();
            kryo.register(NodeStorage.class, 100);
            kryo.register(RelationshipStorage.class, 200);
            kryo.register(CreateOperation.class, 250);
            kryo.register(DeleteOperation.class, 300);
            kryo.register(UpdateOperation.class, 350);
            kryo.register(LoadSensor.LoadDesc.class, 400);
            return kryo;
        };

        /**
         * Creates a realistic operation.
         * @param client the client to assign it to.
         * @param commitAfter num of operations to commit after.
         * @param seed the seed.
         * @param percOfWrites the perc of writes.
         */
        public RealisticOperation(@NotNull final BAGClient client, final int commitAfter, final int seed, final double percOfWrites)
        {
            this.client = client;
            this.commitAfter = commitAfter;
            this.seed = seed;
            this.percOfWrites = percOfWrites;
        }

        /**
         * Load all graph relations.
         * @return list of graph relations.
         * @throws IOException an IOException if failed to load.
         */
        private List<GraphRelation> loadGraphRelations() throws IOException
        {
            final String graphLocation = System.getProperty("user.home") + "/thesis/src/testGraphs/social-a-graph.txt";
            final Random rnd = new Random();
            final int linesToLoad = 500;
            final int upperBound = 239736 - 500;
            final List<GraphRelation> list = new ArrayList<GraphRelation>();
            try (FileReader fr = new FileReader(graphLocation); BufferedReader br = new BufferedReader(fr);)
            {
                final int start = rnd.nextInt(upperBound);
                int count = 0;
                String line;
                while ((line = br.readLine()) != null)
                {
                    count += 1;
                    if (count < start)
                    {
                        continue;
                    }

                    if (count > (start + linesToLoad))
                    {
                        break;
                    }

                    final String[] fields = line.split(" ");
                    final GraphRelation item = new GraphRelation();
                    item.origin = fields[0];
                    item.relationName = fields[1];
                    item.destination = fields[2];
                    list.add(item);
                }
            }

            return list;
        }

        /**
         * Implementing Fisher–Yates shuffle
         * @param array the array to shuffle.
         */
        private static void shuffleArray(final byte[] array)
        {
            int index;
            byte temp;
            final Random random = new Random();
            for (int i = array.length - 1; i > 0; i--)
            {
                index = random.nextInt(i + 1);
                temp = array[index];
                array[index] = array[i];
                array[i] = temp;
            }
        }

        /**
         * Run the workload.
         */
        public void run()
        {
            final int maxNodeId = 100000;
            final int minNodeId = 0;

            final int maxRelationShipId = Constants.RELATIONSHIP_TYPES_LIST.length;

            int relIndex = 0;
            int readNodes = 0;
            int readRelations = 0;
            int createNodes = 0;
            int createRelations = 0;
            int updateNodes = 0;
            final int updateRelations = 0;
            int deleteNodes = 0;
            int deleteRelations = 0;
            int commits = 0;
            final List<GraphRelation> loadedRelations;
            try
            {
                loadedRelations = loadGraphRelations();
            }
            catch (final IOException e)
            {
                e.printStackTrace();
                return;
            }

            final byte[] bytes = new byte[1000000];
            final double percentageOfWrites = commits >= 250 ? this.percOfWrites * 2 : this.percOfWrites;

            for (int i = 0; i < bytes.length; i++)
            {
                if (i <= (percentageOfWrites * bytes.length))
                {
                    bytes[i] = 1;
                    continue;
                }
                bytes[i] = 0;
            }

            shuffleArray(bytes);

            final Random random = new Random(RANDOM_SEED + seed);

            long nanos = System.nanoTime();
            final long totalNanos = nanos;

            for (int i = 1; i < bytes.length; i++)
            {
                final GraphRelation currentNode = loadedRelations.get(relIndex);
                relIndex += 1;
                if (relIndex >= loadedRelations.size())
                {
                    relIndex = 0;
                }

                final boolean isRead = bytes[i] == 0;
                RelationshipStorage readRelationship = null;
                NodeStorage readNodeStorage = null;
                IOperation operation = null;

                if (isRead || percentageOfWrites <= 0)
                {
                    final double randomNum = random.nextDouble() * 100 + 1;
                    if (randomNum <= 15.7)
                    {
                        readRelationship = new RelationshipStorage(
                                currentNode.relationName,
                                new NodeStorage(currentNode.origin),
                                new NodeStorage(currentNode.destination));
                        //get relationship
                        readRelations += 1;
                    }
                    else if (randomNum <= 15.7 + 55.4)
                    {
                        readRelationship = new RelationshipStorage(
                                currentNode.relationName,
                                new NodeStorage(),
                                new NodeStorage(currentNode.destination));
                        //get all relationships of a particular node
                        readRelations += 1;
                    }
                    else
                    {
                        readNodeStorage = new NodeStorage(currentNode.origin);
                        //get node
                        readNodes += 1;
                    }
                }
                else
                {
                    Log.getLogger().info("IS WRITE");
                    final double randomNum = random.nextDouble() * 100 + 1;
                    if (randomNum <= 52.5)
                    {
                        operation = new CreateOperation<>(new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId))));
                        //add relationship
                        createRelations += 1;
                    }
                    else if (randomNum <= 52.5 + 9.2)
                    {
                        operation = new DeleteOperation<>(new RelationshipStorage(
                                Constants.RELATIONSHIP_TYPES_LIST[random.nextInt(maxRelationShipId)],
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId))));
                        //delete relationship
                        deleteRelations += 1;
                    }
                    else if (randomNum <= 52.5 + 9.2 + 16.5)
                    {
                        operation = new CreateOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)));
                        //add node
                        createNodes += 1;
                    }
                    else if (randomNum <= 52.5 + 9.2 + 16.5 + 20.7)
                    {
                        operation = new UpdateOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)),
                                new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)));
                        //update node
                        updateNodes += 1;
                    }
                    else
                    {
                        operation = new DeleteOperation<>(new NodeStorage(String.valueOf(random.nextInt(maxNodeId) + minNodeId)));
                        //delete node
                        deleteNodes += 1;
                    }
                }

                if (isRead)
                {
                    Log.getLogger().info("Start read - clientWorkLoads");

                    if (readNodeStorage != null)
                    {
                        client.read(readNodeStorage);
                        try
                        {
                            while (client.getReadQueue().take() != TestClient.FINISHED_READING)
                            {

                            }
                        }
                        catch (final InterruptedException e)
                        {
                                /*
                                 * Intentionally left empty.
                                 */
                        }
                    }

                    if (readRelationship != null)
                    {
                        client.read(readRelationship);
                        try
                        {
                            while (client.getReadQueue().take() != TestClient.FINISHED_READING)
                            {

                            }
                        }
                        catch (final InterruptedException e)
                        {
                                /*
                                 * Intentionally left empty.
                                 */
                        }
                    }
                    Log.getLogger().info("end read - clientWorkLoads");

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
                    else if (operation instanceof CreateOperation)
                    {
                        client.write(null, ((CreateOperation) operation).getObject());
                    }

                    if (client instanceof DirectAccessClient)
                    {
                        try
                        {
                            while (client.getReadQueue().take() != TestClient.FINISHED_READING)
                            {

                            }
                        }
                        catch (final InterruptedException e)
                        {
                            /*
                             * Intentionally left empty.
                             */
                        }
                    }
                }

                if (i % commitAfter == 0)
                {
                    if (!client.hasRead())
                    {
                        client.read(new NodeStorage(true));
                        try
                        {
                            Log.getLogger().info("Start last read - clientWorkLoads");

                            while (client.getReadQueue().take() != TestClient.FINISHED_READING)
                            {

                            }
                            Log.getLogger().info("end last read - clientWorkLoads");

                        }
                        catch (final InterruptedException e)
                        {
                            /*
                             * Intentionally left empty.
                             */
                        }
                    }

                    commitCount++;
                    client.commit();
                    Log.getLogger().info("Start comitting - clientWorkLoads");
                    while(client.isCommitting())
                    {
                        /*
                         * Wait until the commit finished.
                         * Log.getLogger().info("Commit not finished yet");
                        */
                    }
                    Log.getLogger().info("End comitting - clientWorkLoads");
                    commits += 1;
                }

                if (i % 1000 == 0)
                {
                    final double dif = (System.nanoTime() - nanos) / 1000000000.0;
                    System.out.println(String.format("Elapsed: %.3f s\nreadNodes: %d\nreadRelations: %d\n" +
                                    "createNodes: %d\ncreateRelations: %d\nupdateNodes: %d\nupdateRelations: %d\n" +
                                    "deleteNodes: %d\ndeleteRelations: %d\ncommits: %d\n\n", dif, readNodes, readRelations,
                            createNodes, createRelations, updateNodes, updateRelations, deleteNodes, deleteRelations, commits));
                    nanos = System.nanoTime();
                }
            }

            final double dif = (System.nanoTime() - totalNanos) / 1000000000.0;
            System.out.println(String.format("Total Elapsed: %.3f s\nreadNodes: %d\nreadRelations: %d\n" +
                            "createNodes: %d\ncreateRelations: %d\nupdateNodes: %d\nupdateRelations: %d\n" +
                            "deleteNodes: %d\ndeleteRelations: %d\ncommits: %d\n\n", dif, readNodes, readRelations,
                    createNodes, createRelations, updateNodes, updateRelations, deleteNodes, deleteRelations, commits));
        }
    }
}
