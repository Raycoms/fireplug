package main.java.com.bag.reconfiguration.adaptations;

import bftsmart.reconfiguration.ViewManager;
import bftsmart.tom.ServiceProxy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import main.java.com.bag.util.Log;

import java.net.InetSocketAddress;
import java.util.TimerTask;

import static main.java.com.bag.server.GlobalClusterSlave.GLOBAL_CONFIG_LOCATION;
import static main.java.com.bag.util.Constants.PRIMARY_ELECTION_MESSAGE;

/**
 * Class which handles the primary election.
 */
public class AddPrimaryHandler extends TimerTask
{
    /**
     * The kryo object for serialization.
     */
    private final Kryo kryo;

    /**
     * The id to be replaced.
     */
    private final int  idToCheck;

    /**
     * The local cluster id.
     */
    private final int  localClusterId;

    /**
     * The proxy to consult.
     */
    private ServiceProxy proxy;

    /**
     * The id of the local server.
     */
    private final int id;

    /**
     * Create a new primary election handler.
     * @param kryo the kryo object.
     * @param idToCheck the id to replaced.
     * @param localClusterId the local cluster id.
     * @param proxy the proxy.
     * @param id the id of this server.
     */
    public AddPrimaryHandler(final Kryo kryo, final int idToCheck, final int localClusterId, final ServiceProxy proxy, final int id)
    {
        this.kryo = kryo;
        this.idToCheck = idToCheck;
        this.localClusterId = localClusterId;
        this.proxy = proxy;
        this.id = id;
    }

    @Override
    public void run()
    {
        try(ServiceProxy globalProxy = new ServiceProxy(4000 + this.id, GLOBAL_CONFIG_LOCATION))
        {
            Log.getLogger().warn("----------------------------------------------------");
            Log.getLogger().warn("Starting new primary election!");
            Log.getLogger().warn("----------------------------------------------------");

            final Output output = new Output(128);
            kryo.writeObject(output, PRIMARY_ELECTION_MESSAGE);
            kryo.writeObject(output, idToCheck);
            final byte[] returnBytes = output.getBuffer();
            output.close();

            final byte[] response = proxy.invokeOrdered(returnBytes);
            proxy.close();
            proxy = null;


            int newId = -1;
            if (response == null)
            {
                Log.getLogger().error("Null response from primary election message, this is very bad!");
            }
            else
            {
                final Input input = new Input(response);
                newId = kryo.readObject(input, Integer.class);
                input.close();
            }

            if (newId < 0)
            {
                newId = id;
            }
            newId = newId * 3 + localClusterId;
            Log.getLogger().warn("Host with ID: " + newId + " has been elected!");

            Thread.sleep(2000L);

            final ViewManager newGlobalViewManager = new ViewManager(GLOBAL_CONFIG_LOCATION);
            globalProxy.getViewManager().updateCurrentViewFromRepository();

            while (globalProxy.getViewManager().getCurrentViewN() >= 4)
            {
                Thread.sleep(1000L);
                globalProxy.getViewManager().updateCurrentViewFromRepository();
            }

            final InetSocketAddress newPrimaryAddress = globalProxy.getViewManager().getStaticConf().getRemoteAddress(newId);
            if (newPrimaryAddress == null)
            {
                Log.getLogger().warn("Failed adding new cluster member to global cluster! Id: " + newId);
            }
            else
            {
                newGlobalViewManager.addServer(newId, newPrimaryAddress.getAddress().getHostAddress(), newPrimaryAddress.getPort());
                newGlobalViewManager.executeUpdates();
                Thread.sleep(2000L);
                newGlobalViewManager.close();
                Log.getLogger().warn("Finished adding new cluster member " + newId + " to global cluster!");
            }
            globalProxy.close();
        }
        catch(final Exception ex)
        {
            Log.getLogger().warn("Something went wrong electing a new primary", ex);
        }
    }
}
