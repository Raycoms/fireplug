package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;
import main.java.com.bag.util.NodeStorage;

/**
 * Main class which runs the tests
 */
public class RunTests
{

    public static void main(String [] args)
    {
        byte[] b = {10,100,01,1};



        TestClient client1 = new TestClient(1);

        client1.read(new NodeStorage("Anything"));
        //client1.invokeUnordered(b);


        for(int i = 0; i < 10000000; i++)
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
