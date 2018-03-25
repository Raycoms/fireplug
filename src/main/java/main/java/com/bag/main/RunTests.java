package main.java.com.bag.main;

import main.java.com.bag.client.BAGClient;
import main.java.com.bag.client.DirectAccessClient;
import main.java.com.bag.client.TestClient;
import main.java.com.bag.evaluations.ClientWorkLoads;
import main.java.com.bag.util.Log;
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

    /**
     * Main Method to start tests.
     * Needs 5 arguments with 2 optional types.
     * For hierarchical access: "true", serverPartner (int), localcluster (int), shareOfclient (int), percOfwrites (int)
     * For direct access: "false", address (string), serverPort (int), shareOfClient(int), percOfWrites (int)
     * Optional parameters: Low level logging (true/false). Client read mode (0-5)
     * @param args the required input arguments.
     */
    public static void main(final String[] args)
    {
        int localClusterId = 0;
        int serverPartner = 0;
        int shareOfClient = 1;
        final double percOfWrites;

        int serverPort = 80;

        boolean usesBag = true;

        if (args.length < 5)
        {
            Log.getLogger().warn("Usage:\n"+
                    "To use BAG: RunTests true serverPartner localClusterId shareOfClient percOfWrites [lowLevelLogging]\n"+
                    "To use direct access: RunTests false serverAddress serverPort shareOfClient percOfWrites [lowLevelLoggin]\n");
            return;
        }

        usesBag = Boolean.valueOf(args[0]);
        final String serverIp;
        if (usesBag)
        {
            serverPartner = Integer.parseInt(args[1]);
            localClusterId = Integer.parseInt(args[2]);
            serverIp = "127.0.0.1";
        }
        else
        {
            serverIp = args[1];
            serverPort = Integer.parseInt(args[2]);
        }
        shareOfClient = Integer.parseInt(args[3]);
        percOfWrites = Double.parseDouble(args[4]);

        boolean lowLevelLogging = false;
        if (args.length > 5)
        {
            lowLevelLogging = Boolean.parseBoolean(args[5]);
            if (!lowLevelLogging)
            {
                LogManager.getRootLogger().setLevel(Level.WARN);
            }
        }

        int readMode = 2;
        if(args.length > 6)
        {
            readMode = Integer.parseInt(args[6]);
        }

        final BAGClient client;
        if (usesBag)
        {
            client = new TestClient(shareOfClient, serverPartner, localClusterId, readMode);
        }
        else
        {
            client = new DirectAccessClient(serverIp, serverPort);
        }

        final ClientWorkLoads.RealisticOperation clientWorkLoad =
                new ClientWorkLoads.RealisticOperation(client, 10, shareOfClient, percOfWrites);
        clientWorkLoad.run();
    }
}
