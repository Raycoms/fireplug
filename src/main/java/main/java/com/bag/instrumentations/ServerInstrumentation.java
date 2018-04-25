package main.java.com.bag.instrumentations;

import main.java.com.bag.util.Log;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to report performance information on the server
 */
public class ServerInstrumentation
{
    /**
     * Amount of aborts since last reset.
     */
    private AtomicInteger abortedTransactions = new AtomicInteger(0);

    /**
     * Amount of commits since last reset.
     */
    private AtomicInteger committedTransactions = new AtomicInteger(0);

    /**
     * Reads performed during the last measurement
     */
    private AtomicInteger readsPerformed = new AtomicInteger(0);

    /**
     * Writes performed during the last measurement
     */
    private AtomicInteger writesPerformed = new AtomicInteger(0);

    /**
     * Lock used to synchronize writes to the results file.
     */
    private final Object resultsFileLock = new Object();

    /**
     * Minutes elapsed since the start.
     */
    private int minutesElapsed;

    /**
     * Timer for the instrumentation.
     */
    private final Timer instrumentationTimer = new Timer();

    public ServerInstrumentation(final int id)
    {
        minutesElapsed = 0;
        instrumentationTimer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {

                synchronized (resultsFileLock)
                {
                    minutesElapsed += 1;

                    try (final FileWriter file = new FileWriter(System.getProperty("user.home") + "/results" + id + ".txt", true);
                         final BufferedWriter bw = new BufferedWriter(file);
                         final PrintWriter out = new PrintWriter(bw))
                    {
                        //out.print(elapsed + ";");
                        //out.print(abortedTransactions.get() + ";");
                        //out.print(committedTransactions.get() + ";");
                        //out.print(readsPerformed.get() + ";");
                        //out.print(writesPerformed.get() + ";");
                        out.println(readsPerformed.get() + writesPerformed.get());
                        //out.println();

                        System.out.println(String.format("Elapsed: (%d seconds)\nAborted: %d\nCommited: %d\nReads: %d\nWrites: %d\nThroughput: %d\n\n", minutesElapsed, abortedTransactions.get(), committedTransactions.get(), readsPerformed.get(),
                                writesPerformed.get(), readsPerformed.get() + writesPerformed.get()));

                        abortedTransactions = new AtomicInteger(0);
                        committedTransactions = new AtomicInteger(0);
                        readsPerformed = new AtomicInteger(0);
                        writesPerformed = new AtomicInteger(0);
                    }
                    catch (final IOException e)
                    {
                        Log.getLogger().info("Problem while writing to file!", e);
                    }
                }
            }
        }, 1000, 1000);
    }

    public void updateCounts(final int writes, final int reads, final int commits, final int aborts)
    {
        if (writes > 0)
        {
            writesPerformed.addAndGet(writes);
        }
        if (reads > 0)
        {
            readsPerformed.addAndGet(reads);
        }
        if (commits > 0)
        {
            committedTransactions.addAndGet(commits);
        }
        if (aborts > 0)
        {
            abortedTransactions.addAndGet(aborts);
        }
    }
}
