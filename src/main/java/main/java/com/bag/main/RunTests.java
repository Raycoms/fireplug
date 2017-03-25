package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Main class which runs the tests
 */
public class RunTests
{
    /**
     * Location of the testGraph.
     */
    private static final String GRAPH_LOCATION = "/home/ray/IdeaProjects/BAG - Byzantine fault-tolerant Architecture for Graph database/src/testGraphs/social-a-graph.txt";

    /**
     * Hide the implicit constructor to evit instantiation of this class.
     */
    private RunTests()
    {
        /*
         * Intentionally left empty.
         */
    }

    public static void main(String[] args)
    {
        int localClusterId = 0;
        int serverPartner = 0;
        int numOfLocalCLients = 10;
        int numOfClientSimulators = 3;
        int shareOfClient = 1;

        if (args.length > 4)
        {
            serverPartner = Integer.parseInt(args[0]);
            localClusterId = Integer.parseInt(args[1]);
            numOfLocalCLients = Integer.parseInt(args[2]);
            numOfClientSimulators = Integer.parseInt(args[3]);
            shareOfClient = Integer.parseInt(args[4]);
        }


            for (int i = 1; i <= numOfLocalCLients; i++)
            {
                try (TestClient client = new TestClient(i, serverPartner, localClusterId))
                {
                    MassiveNodeInsertThread runnable = new MassiveNodeInsertThread(client, numOfClientSimulators * numOfLocalCLients, shareOfClient * i, 10, 100000);
                    runnable.run();
                }
            }
    }

    static class MassiveNodeInsertThread implements Runnable
    {
        private final TestClient client;
        private final int startAt;
        private final int stopAt;
        private final int commitAfter;

        public MassiveNodeInsertThread(@NotNull final TestClient client, final int share, final int start, final int commitAfter, final int size)
        {
            this.client = client;
            startAt = start * (size/share) + 1;
            stopAt = startAt + (size/share) - 1;
            this.commitAfter = commitAfter;
        }

        @Override
        public void run()
        {
            int written = 0;
            for (int i = startAt; i <= stopAt; i++)
            {
                written++;
                client.write(null, new NodeStorage(Integer.toString(i)));
                if(written >= commitAfter)
                {
                    client.commit();
                }
            }
        }
    }

    static class MassiveRelationShipInsertThread implements Runnable
    {
        private final TestClient client;
        private final int commitAfter;
        private final int share;
        private final int start;

        public MassiveRelationShipInsertThread(@NotNull final TestClient client, final int share, final int start, final int commitAfter)
        {
            this.client = client;
            this.share = share;
            this.start = start;
            this.commitAfter = commitAfter;
        }

        @Override
        public void run()
        {
            try(FileReader fr = new FileReader(GRAPH_LOCATION); BufferedReader br = new BufferedReader(fr);)
            {
                final long size = br.lines().count();
                final long totalShare = size / share;
                final long startAt = start * totalShare + 1;
                br.skip(startAt - 1);

                int readLines = 0;
                int writtenLines = 0;
                String sCurrentLine;
                while ((sCurrentLine = br.readLine()) != null)
                {
                    final String[] ids = sCurrentLine.split(" ");

                    if(ids.length < 3)
                    {
                        continue;
                    }

                    readLines++;
                    writtenLines++;
                    client.write(null, new RelationshipStorage(ids[1], new NodeStorage(ids[0]), new NodeStorage(ids[2])));
                    if(readLines >= totalShare)
                    {
                        client.commit();
                        break;
                    }

                    if(writtenLines >= commitAfter)
                    {
                        client.commit();
                    }
                    Log.getLogger().info(sCurrentLine);
                }
            }
            catch (IOException e)
            {
                Log.getLogger().warn("Error reading file", e);
            }
        }
    }

    private static void testOld(TestClient client1)
    {
        Map<String, Object> carol = new HashMap<>();
        carol.put("Name", "Caroliny");
        carol.put("Surname", "Goulart");
        carol.put("Age", 22);

        Map<String, Object> ray = new HashMap<>();
        ray.put("Name", "Ray");
        ray.put("Surname", "Neiheiser");
        ray.put("Age", 25);


        client1.read(new NodeStorage("JustToGetAValidSnapshotId"));
        client1.write(new NodeStorage("Person", carol), new NodeStorage("Person", ray));
        client1.read(new NodeStorage("Person", carol));


        //Relationship read
        //client1.read(new RelationshipStorage("Loves", new NodeStorage("Person"), new NodeStorage("Person")));

        //Fill the DB with Person nodes.
        //writeSomeNodes(client1);

        //This deletes all "Person" nodes.
        //client1.write(new NodeStorage("Person"), null);

        //client1.read(new NodeStorage("Person", carol));
        //client1.read(new NodeStorage("Person", ray));

        //Create relationship
        //client1.write(null, new RelationshipStorage("Loves", new NodeStorage("Person", carol),  new NodeStorage("Person", ray)));
        //client1.write(null, new RelationshipStorage("Loves", new NodeStorage("Person", ray),  new NodeStorage("Person", carol)));

        for (int i = 0; i < 1000; i++)
        {
            try
            {
                Thread.sleep(10);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        client1.commit();

        for (int i = 0; i < 100000; i++)
        {
            try
            {
                Thread.sleep(10);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        client1.close();
    }

    private static void writeSomeNodes(final TestClient client1)
    {
        Map<String, Object> carol = new HashMap<>();
        carol.put("Name", "Caroliny");
        carol.put("Surname", "Goulart");
        carol.put("Age", 22);

        Map<String, Object> ray = new HashMap<>();
        ray.put("Name", "Ray");
        ray.put("Surname", "Neiheiser");
        ray.put("Age", 25);

        List<HashMap<String, Object>> maps = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 100; i++)
        {
            HashMap<String, Object> tempMap = new HashMap<>();
            tempMap.put("Name", getRandomName(random));
            tempMap.put("Surname", getRandomName(random));
            tempMap.put("Age", random.nextInt(100));
            maps.add(tempMap);
        }


        client1.write(null, new NodeStorage("Person", carol));
        client1.write(null, new NodeStorage("Person", ray));

        for (HashMap<String, Object> map : maps)
        {
            client1.write(null, new NodeStorage("Person", map));
        }
    }

    private static String getRandomName(Random random)
    {
        String[] names = new String[] {"Tyler", "Jenelle", "Eden", "Rene", "Trinidad", "Mikaela", "Mandi", "Dwight", "Shelia", "Adolph",
                "Marybelle", "Krista", " Misty", "Ling", "Andera", "Lilliana", "Bridgette", "Ona", "Walter", "Akiko", "Tijuana", "Maribel",
                "Eleni", "Marcus", "Shaina", "Bobbie", "Darrick", "Rayford", "Trenton", "Ilona", "Oma", "Jacinda", "Chelsie", "Henry",
                "Maren", "Louise", "Tayna", "Dennis", "Ashleigh", "Providencia", "Osvaldo", "Jeanie",
                "Neta", "Brittni", "Lindsey", "Pearline", "Kelsey", "Amiee", "Candance", "Earlean", "Tyler", "Jenelle", "Eden", "Rene", "Trinidad", "Mikaela", "Mandi", "Dwight",
                "Shelia", "Adolph",
                "Marybelle", "Krista", " Misty", "Ling", "Andera", "Lilliana", "Bridgette", "Ona", "Walter", "Akiko", "Tijuana", "Maribel",
                "Eleni", "Marcus", "Shaina", "Bobbie", "Darrick", "Rayford", "Trenton", "Ilona", "Oma", "Jacinda", "Chelsie", "Henry",
                "Maren", "Louise", "Tayna", "Peter", "Ashleigh", "Providencia", "Osvaldo", "Jeanie",
                "Neta", "Brittni", "Lindsey", "Pearline", "Kelsey", "Amiee", "Candance", "Earlean"};

        return names[random.nextInt(names.length)];
    }
}
