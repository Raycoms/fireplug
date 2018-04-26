package main.java.com.bag.reconfiguration.sensors;

import bftsmart.reconfiguration.ViewManager;
import bftsmart.tom.ServiceProxy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import main.java.com.bag.reconfiguration.adaptations.AddBftPrimaryHandler;
import main.java.com.bag.server.LocalClusterSlave;
import main.java.com.bag.util.Log;

import java.util.Timer;
import java.util.TimerTask;

import static main.java.com.bag.server.GlobalClusterSlave.GLOBAL_CONFIG_LOCATION;
import static main.java.com.bag.util.Constants.*;

//Todo in the future, the crash detection sensor and the bft detection sensor have to exchange their variables to make sure they work along.
//Todo for now it's not a problem because the bft detection sensor will also detect crashes (even though with a delay).
/**
 * Sensor to detect crashes. It pings a specific server and checks if available, if not available it removes it from the view and updates the view.
 * This is run periodically.
 */
public class BftDetectionSensor extends TimerTask
{
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
    private final Timer electionTimer = new Timer();

    /**
     * The id of the primary.
     */
    private int primaryId = 0;

    /**
     * Proxy to the global cluster.
     */
    private final ServiceProxy globalProxy;

    /**
     * Connection to the local cluster slave which started this task.
     */
    private final LocalClusterSlave localSlave;

    /**
     * Creates a crash detection sensor.
     *
     * @param proxy          the proxy it uses.
     * @param configLocation the configuration location.
     * @param id             it's id.
     * @param kryo           the kryo object.
     */
    public BftDetectionSensor(final ServiceProxy proxy, final String configLocation, final int id, final Kryo kryo, final int localClusterId, final LocalClusterSlave slave)
    {
        this.proxy = proxy;
        this.configLocation = configLocation;
        this.id = id;
        this.kryo = kryo;
        this.localClusterId = localClusterId;
        this.globalProxy = new ServiceProxy(5000 + (this.id * 3 + localClusterId), GLOBAL_CONFIG_LOCATION);
        this.localSlave = slave;
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
        globalProxy.getViewManager().updateCurrentViewFromRepository();

        final Output output = new Output(128);
        kryo.writeObject(output, AM_I_OUTDATED_MESSAGE);
        kryo.writeObject(output, localSlave.getGlobalSnapshotId());
        final byte[] returnBytes = output.getBuffer();
        output.close();

        final byte[] response = globalProxy.invokeUnordered(returnBytes);

        boolean needsReconfiguration = false;
        if (response == null)
        {
            Log.getLogger().error("Null response from the bft detection sensor, this is very bad!");
        }
        else
        {
            final Input input = new Input(response);
            needsReconfiguration = kryo.readObject(input, Boolean.class);
            input.close();
        }

        if (needsReconfiguration)
        {
            Log.getLogger().error("------------------------------------------");
            Log.getLogger().error("Detected byzantine primary!!!");
            Log.getLogger().warn("Starting reconfiguration at cluster: " + configLocation + " with config: "  + configLocation);
            Log.getLogger().error("------------------------------------------");
            try
            {
                // Removing the server from the local view (we don't trust it anymore).
                final ViewManager viewManager = new ViewManager(configLocation);
                viewManager.removeServer(primaryId);
                viewManager.executeUpdates();
                Thread.sleep(2000L);
                viewManager.close();
                Log.getLogger().warn("Finished reconfiguration at cluster: " + configLocation + " with config: "  + configLocation);
                proxy.getViewManager().updateCurrentViewFromRepository();
                Log.getLogger().warn("Finished updating old view at cluster: " + configLocation);
                electionTimer.schedule(new AddBftPrimaryHandler(kryo, primaryId, localClusterId, proxy, id, this), 5000);
            }
            catch (final InterruptedException e)
            {
                Log.getLogger().error("Unable to reconfigure at cluster: " + configLocation, e);
            }
            catch (final NullPointerException ex)
            {
                Log.getLogger().warn("NPE - restarting!", ex);
                run();
            }
        }
    }

    /**
     * Update the primary Id, after a new election.
     * @param primaryId the primary id to set.
     */
    public void setPrimaryId(final int primaryId)
    {
        this.primaryId = primaryId;
    }
}
