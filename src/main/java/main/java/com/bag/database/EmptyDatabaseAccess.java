package main.java.com.bag.database;

import main.java.com.bag.exceptions.OutDatedDataException;
import main.java.com.bag.database.interfaces.IDatabaseAccess;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Empty DatabaseAccess for testing only.
 */
public class EmptyDatabaseAccess implements IDatabaseAccess
{
    final Random random = new Random();

    @Override
    public void start()
    {
        Log.getLogger().error("Starting EMPTY Database");
        /*
         * Nothing to do here.
         */
    }

    @Override
    public String toString()
    {
        return "URG EMPTY Database!!!";
    }

    @Override
    public void terminate()
    {
        /*
         * Nothing to do here.
         */
    }

    @Override
    public boolean compareRelationship(final RelationshipStorage storage)
    {
        return true;
    }

    @Override
    public boolean compareNode(final NodeStorage storage)
    {
        return true;
    }

    @Override
    public boolean applyUpdate(final NodeStorage key, final NodeStorage value, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public boolean applyCreate(final NodeStorage storage, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public boolean applyDelete(final NodeStorage storage, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public boolean applyUpdate(final RelationshipStorage key, final RelationshipStorage value, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public boolean applyCreate(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public boolean applyDelete(final RelationshipStorage storage, final long snapshotId)
    {
        try
        {
            Thread.sleep(random.nextInt(10));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }
        return true;
    }

    @Override
    public List<Object> readObject(final Object identifier, final long localSnapshotId) throws OutDatedDataException
    {
        final List<Object> returnList = new ArrayList<>();
        returnList.add(identifier);

        try
        {
            Thread.sleep(random.nextInt(5));
        }
        catch (final InterruptedException e)
        {
            /*
             * Ignore this.
             */
        }

        return returnList;
    }

    @Override
    public boolean shouldFollow(final int sequence)
    {
        return true;
    }
}
