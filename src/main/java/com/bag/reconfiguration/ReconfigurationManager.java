package com.bag.reconfiguration;

import com.bag.reconfiguration.sensors.LoadSensor;
import com.bag.server.GlobalClusterSlave;
import com.bag.util.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import static com.bag.util.Constants.BORDER_CPU_USAGE;

public class ReconfigurationManager extends TimerTask
{
    //todo this will receive the data from the global cluster (the messages it receives)
    /**
     * Hashmap which has the information about all local cluster performance.
     * First map contains all local cluster.
     * Second map contains the local cluster in greater detail.
     */
    private final HashMap<Integer, Map<Integer, LoadSensor.LoadDesc>> performanceMap = new HashMap<>();

    /**
     * The size of the primary cluster.
     */
    private final int primaryClusterSize;

    /**
     * The instance of the last series of checks.
     */
    private int instance = 0;

    /**
     * The global cluster slave the manager runs on.
     */
    private final GlobalClusterSlave primary;

    /**
     * Create a new ReconfigurationManager task.
     * @param primary the replica it belongs to.
     * @param primaryClusterSize the size of the global cluster.
     */
    public ReconfigurationManager(final GlobalClusterSlave primary, final int primaryClusterSize)
    {
        this.primary = primary;
        this.primaryClusterSize = primaryClusterSize;
    }

    /**
     * Add a performanceMap of a local cluster to the global performance map.
     * @param localClusterId the local cluster id.
     * @param map the map.
     * @param instance the instance the map is at.
     */
    public void addToPerformanceMap(final int localClusterId, final Map<Integer, LoadSensor.LoadDesc> map, final int instance)
    {
        if (instance != 0 && instance != this.instance)
        {
            performanceMap.clear();
            this.instance = instance;
        }
        performanceMap.put(localClusterId, map);
    }

    @Override
    public void run()
    {
        if (performanceMap.size() >= (primaryClusterSize/2)+1)
        {
            Log.getLogger().warn("Starting to check slave performance");
            int slavesNeedingReconfiguration = 0;
            for (final Map<Integer, LoadSensor.LoadDesc> map : performanceMap.values())
            {
                for(final LoadSensor.LoadDesc load : map.values())
                {
                    Log.getLogger().warn("Detected CPU usage: " + load.getCpuUsage());
                    if (load.getCpuUsage() > BORDER_CPU_USAGE)
                    {
                        slavesNeedingReconfiguration++;
                    }

                    if (slavesNeedingReconfiguration >= ((primaryClusterSize * map.size()) / 2))
                    {
                        Log.getLogger().warn("Requiring to change the algorithm!!!: " + load.getCpuUsage());
                        //primary.adaptAlgorithm();
                        //todo trigger reconfiguration
                    }
                }
            }
        }
    }
}
