package main.java.com.bag.reconfiguration.sensors;

import main.java.com.bag.util.Log;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.TimerTask;

/**
 * Load sensor which checks for the load on the machine.
 */
public class LoadSensor extends TimerTask
{
    private final long maxMemory;
    private long allocatedMemory;
    private long freeMemory;
    private double cpuUsage;

    //todo in the future make this an extra program which delivers it via socket.
    public LoadSensor()
    {
        final Runtime runtime = Runtime.getRuntime();
        this.maxMemory = runtime.maxMemory();
    }

    @Override
    public void run()
    {
        final Runtime runtime = Runtime.getRuntime();

        this.allocatedMemory = runtime.totalMemory();
        this.freeMemory = runtime.freeMemory();

        try
        {
            cpuUsage = getProcessCpuLoad();
            Log.getLogger().warn("CPU: Current load: " + cpuUsage);
            Log.getLogger().warn("Memory: " + maxMemory + " / " + allocatedMemory + " / " + freeMemory);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    public static double getProcessCpuLoad() throws Exception
    {
        final MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
        final ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
        final AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

        if (list.isEmpty())
        {
            return Double.NaN;
        }

        final Attribute att = (Attribute) list.get(0);
        final Double value  = (Double)att.getValue();

        // usually takes a couple of seconds before we get real values
        if (value == -1.0)
        {
            return Double.NaN;
        }
        // returns a percentage value with 1 decimal point precision
        return ((int)(value * 1000) / 10.0);
    }

    /**
     * Getter for the max memory.
     * @return the max memory.
     */
    public long getMaxMemory()
    {
        return maxMemory;
    }

    /**
     * Getter for the allocated memory.
     * @return the allocated memory.
     */
    public long getAllocatedMemory()
    {
        return allocatedMemory;
    }

    /**
     * Getter for the free memory.
     * @return the free memory.
     */
    public long getFreeMemory()
    {
        return freeMemory;
    }

    /**
     * Getter for the cpu usage.
     * @return the cpu usage.
     */
    public double getCpuUsage()
    {
        return cpuUsage;
    }
}
