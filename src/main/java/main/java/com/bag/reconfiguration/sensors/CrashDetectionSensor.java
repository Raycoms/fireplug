package main.java.com.bag.reconfiguration.sensors;

import bftsmart.reconfiguration.ViewManager;
import bftsmart.tom.ServiceProxy;
import com.esotericsoftware.kryo.Kryo;
import main.java.com.bag.reconfiguration.AddPrimaryHandler;
import main.java.com.bag.util.Log;

import java.io.DataOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import static main.java.com.bag.server.GlobalClusterSlave.GLOBAL_CONFIG_LOCATION;
import static main.java.com.bag.util.Constants.*;

/**
 * Sensor to detect crashes. It pings a specific server and checks if available, if not available it removes it from the view and updates the view.
 * This is run periodically.
 */
public class CrashDetectionSensor extends TimerTask
{
    /**
     * The id it should check.
     */
    private int idToCheck;

    /**
     * Proxy to send messages and get view.
     */
    private final ServiceProxy proxy;

    /**
     * The config location on disk.
     */
    private final String configLocation;

    /**
     * It's id.
     */
    private final int id;

    /**
     * Kryo object for serialization.
     */
    private final Kryo kryo;

    /**
     * The id of the local cluster.
     */
    private final int localClusterId;

    /**
     * Timer to schedule the election.
     */
    final Timer electionTimer = new Timer();

    /**
     * Creates a crash detection sensor.
     *
     * @param idToCheck      the id it checks.
     * @param proxy          the proxy it uses.
     * @param configLocation the configuration location.
     * @param id             it's id.
     * @param kryo           the kryo object.
     */
    public CrashDetectionSensor(final int idToCheck, final ServiceProxy proxy, final String configLocation, final int id, final Kryo kryo, final int localClusterId)
    {
        this.idToCheck = idToCheck;
        this.proxy = proxy;
        this.configLocation = configLocation;
        this.id = id;
        this.kryo = kryo;
        this.localClusterId = localClusterId;
    }

    @Override
    public void run()
    {
        if (proxy == null || id == idToCheck)
        {
            Log.getLogger().warn("Proxy became null, not executing analysis!");
            return;
        }

        proxy.getViewManager().updateCurrentViewFromRepository();

        if (proxy.getViewManager().getCurrentView().getProcesses()[proxy.getViewManager().getCurrentView().getProcesses().length - 1] < idToCheck)
        {
            idToCheck = 0;
        }

        Log.getLogger().warn("Servers : " + Arrays.toString(proxy.getViewManager().getCurrentView().getProcesses()) + " at: " + id + " checking on: " + idToCheck);

        final String cluster;
        if (configLocation.contains(GLOBAL_CLUSTER))
        {
            cluster = GLOBAL_CLUSTER;
        }
        else
        {
            cluster = LOCAL_CLUSTER;
        }

        final InetSocketAddress address = proxy.getViewManager().getCurrentView().getAddress(idToCheck);
        boolean needsReconfiguration = false;
        try (Socket socket = new Socket(address.getHostName(), address.getPort()))
        {
            new DataOutputStream(socket.getOutputStream()).writeInt(id + 1);
            Log.getLogger().info("Connection established");
        }
        catch (final ConnectException ex)
        {
            if (ex.getMessage().contains("refused"))
            {
                needsReconfiguration = true;
            }
        }
        catch (final Exception ex)
        {
            //This here is normal in the global cluster, let's ignore this.
            Log.getLogger().warn("Something is going down here", ex);
        }

        if (needsReconfiguration)
        {
            Log.getLogger().warn("Starting reconfiguration at cluster: " + cluster);
            try
            {
                /*if (cluster.equalsIgnoreCase(GLOBAL_CLUSTER))
                {
                    Thread.sleep(2000L);
                }*/
                final ViewManager viewManager = new ViewManager(configLocation);
                viewManager.removeServer(idToCheck);
                viewManager.executeUpdates();
                Thread.sleep(2000L);
                viewManager.close();
                Log.getLogger().warn("Finished reconfiguration at cluster: " + cluster);
                proxy.getViewManager().updateCurrentViewFromRepository();
                Log.getLogger().warn("Finished updating old view at cluster: " + cluster);
                if (cluster.equalsIgnoreCase(LOCAL_CLUSTER))
                {
                    electionTimer.schedule(new AddPrimaryHandler(kryo, idToCheck, localClusterId, proxy, id), 1000);
                }
                idToCheck += 1;
            }
            catch (final InterruptedException e)
            {
                Log.getLogger().error("Unable to reconfigure at cluster: " + cluster, e);
            }
            catch (final NullPointerException ex)
            {
                Log.getLogger().warn("NPE - restarting!", ex);
                run();
            }
        }
    }
}
