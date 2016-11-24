package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.*;

/**
 * Main class which runs the tests
 */
public class RunTests
{
    /**
     * Hide the implicit constructor to evit instantiation of this class.
     */
    private RunTests()
    {
        /*
         * Intentionally left empty.
         */
    }

    public static void main(String [] args)
    {
        int serverPartner = 0;
        if(args.length > 0)
        {
            serverPartner  = Integer.parseInt(args[0]);
        }
        try (TestClient client1 = new TestClient(1, serverPartner))
        {
            HashMap<String, Object> find = new HashMap<>();
            find.put("Age", 70);
            find.put("Surname", "Oma");

            HashMap<String, Object> find2 = new HashMap<>();
            find2.put("Age", 70);

            //client1.read(new NodeStorage("JustToGetAValidSnapshotId"));


            client1.read(new NodeStorage("Person" , find));

            //client1.write(new NodeStorage("Person", find), new NodeStorage("Person", find2));

            //Relationship read
            //client1.read(new RelationshipStorage("Loves", new NodeStorage("Person"), new NodeStorage("Person")));

            //Fill the DB with Person nodes.
            //writeSomeNodes(client1);

            //This deletes all "Person" nodes.
            //client1.write(new NodeStorage("Person"), null);

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
                "Maren", "Louise", "Tayna", "Denis", "Ashleigh", "Providencia", "Osvaldo", "Jeanie",
                "Neta", "Brittni", "Lindsey", "Pearline", "Kelsey", "Amiee", "Candance", "Earlean", "Tyler", "Jenelle", "Eden", "Rene", "Trinidad", "Mikaela", "Mandi", "Dwight", "Shelia", "Adolph",
                "Marybelle", "Krista", " Misty", "Ling", "Andera", "Lilliana", "Bridgette", "Ona", "Walter", "Akiko", "Tijuana", "Maribel",
                "Eleni", "Marcus", "Shaina", "Bobbie", "Darrick", "Rayford", "Trenton", "Ilona", "Oma", "Jacinda", "Chelsie", "Henry",
                "Maren", "Louise", "Tayna", "Denis", "Ashleigh", "Providencia", "Osvaldo", "Jeanie",
                "Neta", "Brittni", "Lindsey", "Pearline", "Kelsey", "Amiee", "Candance", "Earlean"};

        return names[random.nextInt(names.length)];
    }
}
