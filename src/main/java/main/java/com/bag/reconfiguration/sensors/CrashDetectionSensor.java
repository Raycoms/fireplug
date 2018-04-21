package main.java.com.bag.reconfiguration.sensors;

import bftsmart.reconfiguration.ViewManager;
import bftsmart.tom.ServiceProxy;
import main.java.com.bag.util.Log;

import java.io.DataOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.TimerTask;

/**
 * Sensor to detect crashes. It pings a specific server and checks if available, if not available it removes it from the view and updates the view.
 * This is run periodically.
 */
public class CrashDetectionSensor extends TimerTask
{
    private int positionToCheck;
    private final ServiceProxy proxy;
    private final String configLocation;
    private final int id;
    public CrashDetectionSensor(final int positionToCheck, final ServiceProxy proxy, final String configLocation, final int id)
    {
        this.positionToCheck = positionToCheck;
        this.proxy = proxy;
        this.configLocation = configLocation;
        this.id = id;
    }

    @Override
    public void run()
    {
        if (proxy == null)
        {
            Log.getLogger().warn("Proxy became null, not executing analysis!");
            return;
        }

        proxy.getViewManager().updateCurrentViewFromRepository();

        if (positionToCheck >= proxy.getViewManager().getCurrentView().getProcesses().length)
        {
            positionToCheck = 0;
        }

        final int idToCheck = proxy.getViewManager().getCurrentViewProcesses()[positionToCheck];
        Log.getLogger().warn("Servers : " + Arrays.toString(proxy.getViewManager().getCurrentView().getProcesses()) + " at: " + id + " checking on: " + idToCheck);

        final String cluster;
        if (configLocation.contains("global"))
        {
            cluster = "global";
        }
        else
        {
            cluster = "local";
        }

        final InetSocketAddress address = proxy.getViewManager().getCurrentView().getAddress(idToCheck);
        boolean needsReconfiguration = false;
        try(Socket socket = new Socket(address.getHostName(), address.getPort()))
        {
            new DataOutputStream(socket.getOutputStream()).writeInt(id+1);
            Log.getLogger().info("Connection established");
        }
        catch(final ConnectException ex)
        {
            if (ex.getMessage().contains("refused"))
            {
                needsReconfiguration = true;
            }
        }
        catch (final Exception ex)
        {
            //This here is normal in the global cluster, let's ignore this.
            Log.getLogger().warn(ex);
        }

        if (needsReconfiguration)
        {
            Log.getLogger().warn("Starting reconfiguration at cluster: " + cluster);
            try
            {
                final ViewManager viewManager = new ViewManager(configLocation);
                viewManager.removeServer(idToCheck);
                viewManager.executeUpdates();
                Thread.sleep(2000L);
                viewManager.close();
                positionToCheck += 1;
                Log.getLogger().warn("Finished reconfiguration at cluster: " + cluster);
                proxy.getViewManager().updateCurrentViewFromRepository();
                Log.getLogger().warn("Finished updating old view at cluster: " + cluster);
            }
            catch (final InterruptedException e)
            {
                Log.getLogger().error("Unable to reconfigure at cluster: " + cluster, e);
            }
        }
    }
}
