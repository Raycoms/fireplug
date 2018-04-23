package main.java.com.bag.instrumentations;

import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
     * Time of last commit.
     */
    private long lastCommit;

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
     * Server's id
     */
    private final int id;

    /**
     * Minutes elapsed since the start.
     */
    private int minutesElapsed;

    public ServerInstrumentation(final int id)
    {
        this.id = id;
        lastCommit = System.nanoTime();
        minutesElapsed = 0;
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

        if (((System.nanoTime() - lastCommit) / Constants.NANO_TIME_DIVIDER) >= 1.0)
        {
            synchronized (resultsFileLock)
            {
                final double elapsed = ((System.nanoTime() - lastCommit) / Constants.NANO_TIME_DIVIDER);
                if (elapsed >= 1.0)
                {
                    lastCommit = System.nanoTime();
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

                        System.out.println(String.format("Elapsed: %.3fs (%d minutes)\nAborted: %d\nCommited: %d\nReads: %d\nWrites: %d\nThroughput: %d\n\n",
                                elapsed, minutesElapsed, abortedTransactions.get(), committedTransactions.get(), readsPerformed.get(),
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
        }
    }
}
