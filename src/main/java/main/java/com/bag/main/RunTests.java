package main.java.com.bag.main;

import main.java.com.bag.client.BAGClient;
import main.java.com.bag.client.DirectAccessClient;
import main.java.com.bag.client.TestClient;
import main.java.com.bag.evaluations.ClientWorkLoads;
import main.java.com.bag.evaluations.NettyClient;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.util.*;

/**
 * Main class which runs the tests
 */
public class RunTests
{
    /**
     * Hide the implicit constructor to evit instantiation of this class.
     */
    public RunTests()
    {
        /*
         * Intentionally left empty.
         */
    }

    public static void main(String[] args)
    {
        int localClusterId = 0;
        int serverPartner = 0;
        int shareOfClient = 1;
        boolean lowLevelLogging = false;
        double percOfWrites = 0.02;

        String serverIp = "127.0.0.1";
        int serverPort = 80;

        boolean usesBag = true;

        if (args.length < 5) {
            System.out.println("Usage:\n"+
                "To use BAG: RunTests true serverPartner localClusterId shareOfClient percOfWrites [lowLevelLogging]\n"+
                "To use direct access: RunTests false serverAddress serverPort shareOfClient percOfWrites [lowLevelLoggin]\n");
            return;
        }

        usesBag = Boolean.valueOf(args[0]);
        if (usesBag)
        {
            serverPartner = Integer.parseInt(args[1]);
            localClusterId = Integer.parseInt(args[2]);
        } else
        {
            serverIp = args[1];
            serverPort = Integer.parseInt(args[2]);
        }
        shareOfClient = Integer.parseInt(args[3]);
        percOfWrites = Double.parseDouble(args[4]);
        if (args.length > 5)
            lowLevelLogging = Boolean.parseBoolean(args[5]);

        if (!lowLevelLogging)
            LogManager.getRootLogger().setLevel(Level.WARN);

        BAGClient client;
        if (usesBag)
            client = new TestClient(shareOfClient, serverPartner, localClusterId);
        else
            client = new DirectAccessClient(serverIp, serverPort);

        final ClientWorkLoads.RealisticOperation clientWorkLoad =
                new ClientWorkLoads.RealisticOperation(client, 10, shareOfClient, percOfWrites);
        clientWorkLoad.run();
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
        client1.reset();
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
