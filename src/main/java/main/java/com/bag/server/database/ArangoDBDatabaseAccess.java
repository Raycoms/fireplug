package main.java.com.bag.server.database;

import main.java.com.bag.server.database.interfaces.IDatabaseAccess;

import java.util.List;

/**
 * Created by ray on 10/12/16.
 */
public class ArangoDBDatabaseAccess implements IDatabaseAccess
{
    public void start(int id)
    {

    }

    public void terminate()
    {

    }

    @Override
    public boolean equalHash(final List readSet)
    {
        return false;
    }
}
