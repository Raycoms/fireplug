package main.java.com.bag.main;

import main.java.com.bag.client.TestClient;
import main.java.com.bag.server.database.Neo4jDatabaseAccess;

/**
 * Main class which runs the tests
 */
public class RunTests
{

    public static void main(String [] args)
    {
        byte[] b = {10,100,01,1};

        Neo4jDatabaseAccess neo4j = new Neo4jDatabaseAccess();
        neo4j.start();

        TestClient client1 = new TestClient(1);

        client1.invokeUnordered(b);


        for(int i = 0; i < 100000; i++)
        {

        }

        neo4j.terminate();
        client1.close();





    }
}
