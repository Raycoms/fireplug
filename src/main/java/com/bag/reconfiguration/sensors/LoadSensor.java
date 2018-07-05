package com.bag.reconfiguration.sensors;

import bftsmart.tom.ServiceProxy;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.bag.util.Log;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.TimerTask;

import static com.bag.util.Constants.PERFORMANCE_UPDATE_MESSAGE;

/**
 * Load sensor which checks for the load on the machine.
 */
public class LoadSensor extends TimerTask
{
    /**
     * The load description.
     */
    private final LoadDesc desc;

    /**
     * Lock on the load desc object.
     */
    private final Object descLock = new Object();

    /**
     * Kryo object to serialize.
     */
    private final Kryo kryo;

    /**
     * The service proxy of the replica.
     */
    private final ServiceProxy proxy;

    /**
     * The id of the localhost.
     */
    private final int localHostId;

    /**
     * Instance the load sensor operates in.
     */
    private int instance = 0;

    //todo in the future make this an extra program which delivers it via socket.
    /**
     * Constructor for the load sensor.
     * @param kryo the kryo instance to serialize.
     * @param proxy the proxy of it to send it.
     * @param localHostId the id of the local host.
     * @param db the db associated to it.
     */
    public LoadSensor(final Kryo kryo, final ServiceProxy proxy, final int localHostId, final String db)
    {
        this.kryo = kryo;
        desc = new LoadDesc(db);
        this.proxy = proxy;
        this.localHostId = localHostId;
    }

    @Override
    public void run()
    {
        synchronized (descLock)
        {
            final Runtime runtime = Runtime.getRuntime();

            desc.incrementInstance();
            desc.allocatedMemory = (desc.allocatedMemory + runtime.totalMemory()) * 2;
            desc.freeMemory = (desc.freeMemory + runtime.freeMemory()) * 2;

            try
            {
                desc.cpuUsage = (desc.cpuUsage + getProcessCpuLoad()) * 2;
                Log.getLogger().info("CPU: Current load: " + desc.cpuUsage);
                Log.getLogger().info("Memory: " + desc.maxMemory + " / " + desc.allocatedMemory + " / " + desc.freeMemory);
            }
            catch (final Exception e)
            {
                Log.getLogger().info(e);
            }

            final Output output = new Output(128);
            kryo.writeObject(output, PERFORMANCE_UPDATE_MESSAGE);
            kryo.writeObject(output, desc);
            kryo.writeObject(output, localHostId);
            final byte[] returnBytes = output.getBuffer();
            output.close();
            proxy.invokeOrdered(returnBytes);
        }
    }

    /**
     * Getter for the load desc object. Takes care of concurrency.
     * @return a copy of the description object for the load.
     */
    public LoadDesc getLoadDesc()
    {
        synchronized (descLock)
        {
            return new LoadDesc(this.desc);
        }
    }

    /**
     * Method taken from: https://stackoverflow.com/questions/74674/how-to-do-i-check-cpu-and-memory-usage-in-java
     *
     * Calculator for CPU load.
     * @return double value or NaN
     * @throws Exception can be thrown on retrieval of the cpu load or name.
     */
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
     * Storage object for the load.
     */
    public static class LoadDesc implements Serializable
    {
        /**
         * Max memory available (estimated).
         */
        private final long maxMemory;

        /**
         * Allocated memory available (estimated).
         */
        private long allocatedMemory;

        /**
         * Free memory available (estimated).
         */
        private long freeMemory;

        /**
         * Cpu usage (estimated).
         */
        private double cpuUsage;

        /**
         * The database of the server.
         */
        private String db;

        /**
         * The instance this LoadDesc belongs to.
         */
        private int instance;

        /**
         * Default constructor for Serialization.
         */
        public LoadDesc()
        {
            this.maxMemory = Runtime.getRuntime().maxMemory();
            this.db = "neo4j";
        }

        /**
         * Load desc constructor.
         */
        public LoadDesc(final String db)
        {
            this.maxMemory = Runtime.getRuntime().maxMemory();
            this.db = db;
        }

        /**
         * Load desc constructor.
         * @param desc the desc to copy to it.
         */
        public LoadDesc(final LoadDesc desc)
        {
            this.maxMemory = desc.maxMemory;
            this.allocatedMemory = desc.allocatedMemory;
            this.freeMemory = desc.freeMemory;
            this.cpuUsage = desc.cpuUsage;
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
         * Storage object for the load.
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

        /**
         * Getter for the db.
         * @return the db string.
         */
        public String getDb()
        {
            return db;
        }

        /**
         * Increments the instance number.
         */
        public void incrementInstance()
        {
            instance++;
        }

        /**
         * Getter for the instance number.
         * @return the instance number.
         */
        public int getInstance()
        {
            return this.instance;
        }
    }
}
