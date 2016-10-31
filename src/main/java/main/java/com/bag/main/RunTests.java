package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.util.NodeStorage;

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
            client1.read(new NodeStorage(""));

            for (int i = 0; i < 10000000; i++)
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
