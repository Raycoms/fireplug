package main.java.com.bag.util;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Class used to measure execution times to help optimizing the code.
 */
public class Instrumentation
{
    private class InstrumentationData
    {
        public String name;
        public int skip;
        public long startTime;
        public long lastMeasure;
        public int count;
        public long totalMeasured;
        public long accumulatedSinceLastOutput;
        public long lastCount;

        public InstrumentationData(String name, int skip)
        {
            this.name = name;
            this.skip = skip;
            this.startTime = 0;
            this.lastMeasure = 0;
            this.count = 0;
            this.totalMeasured = 0;
            this.accumulatedSinceLastOutput = 0;
            this.lastCount = 0;
        }
    }

    private static ThreadLocal<Instrumentation> instance = new ThreadLocal<>();

    public static Instrumentation get()
    {
        Instrumentation obj = instance.get();
        if (obj == null)
        {
            obj = new Instrumentation();
            instance.set(obj);
        }
        return obj;
    }

    public static void s(String name)
    {
        get().start(name);
    }

    public static void f(String name)
    {
        get().finish(name);
    }

    private ConcurrentHashMap<String, InstrumentationData> data;

    private Instrumentation()
    {
        data = new ConcurrentHashMap<>();
    }

    public void addMeasure(String name, int skip)
    {
        data.put(name, new InstrumentationData(name, skip));
    }

    public void start(String name)
    {
        if (!data.containsKey(name))
            addMeasure(name, 0);
        InstrumentationData info = data.get(name);
        info.startTime = System.nanoTime();
    }

    public void finish(String name)
    {
        InstrumentationData info = data.get(name);
        long endTime = System.nanoTime();
        info.lastMeasure = endTime - info.startTime;
        info.totalMeasured += info.lastMeasure;
        info.accumulatedSinceLastOutput += info.lastMeasure;
        info.count += 1;
    }

    public void output()
    {
        output(null);
    }

    public void output(String additionalMessage)
    {
        StringBuilder sb = new StringBuilder();
        for (InstrumentationData info : data.values())
        {
            if (info.count <= 0 || info.lastCount == info.count)
                continue;

            double average;
            double took = (double) info.accumulatedSinceLastOutput / 1000000.0;

            info.accumulatedSinceLastOutput = 0;
            info.lastCount = info.count;

            if (info.count > info.skip)
            {
                average = (double) info.totalMeasured / (double) info.count / 1000000.0;
                sb.append(String.format("%s took %.3fms\n   Rounds: %d\n   Average: %.3fms\n",
                        info.name, took, info.count, average));
            } else
            {
                sb.append(String.format("%s took %.3fms\n   Rounds: %d\n   Average: Warming up!\n",
                        info.name, took, info.count));
            }
        }

        String threadName = Thread.currentThread().getName();
        if (additionalMessage != null)
            Log.getLogger().warn("\n" + threadName + " ---  Performance (" + additionalMessage + "):\n" + sb.toString());
        else
            Log.getLogger().warn("\n" + threadName + " ---  Performance:\n" + sb.toString());
    }
}