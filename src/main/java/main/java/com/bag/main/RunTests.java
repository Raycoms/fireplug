package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.HashMap;
import java.util.Map;

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
        try (TestClient client1 = new TestClient(1))
        {
            client1.read(new NodeStorage("Person"));

            //client1.read(new RelationshipStorage("Loves", new NodeStorage("Person"), new NodeStorage("Person")));

            /*Map<String, Object> carol = new HashMap<>();
            carol.put("Name", "Caroliny");
            carol.put("Surname", "Goulart");
            carol.put("Age", "22");

            Map<String, Object> ray = new HashMap<>();
            carol.put("Name", "Ray");
            carol.put("Surname", "Neiheiser");
            carol.put("Age", "25");

            client1.write(null, new NodeStorage("Person", carol));
            client1.write(null, new NodeStorage("Person", ray));
            client1.write(null, new RelationshipStorage("Loves", new NodeStorage("Person", carol),  new NodeStorage("Person", ray)));
            client1.write(null, new RelationshipStorage("Loves", new NodeStorage("Person", ray),  new NodeStorage("Person", carol)));*/

            for (int i = 0; i < 10000; i++)
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
}
