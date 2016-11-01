package main.java.com.bag.server.database.interfaces;


import java.util.List;

/**
 * Abstract class with required methods for all graph databases.
 */
public abstract interface IDatabaseAccess
{
    /**
     * Method used to start the graph database service.
     * @param id change foldername depending on the id.
     */
    public void start(int id);

    /**
     * Method used to terminate the graph database service.
     */
    public void terminate();

    /**
     * Method used to check if the hashes inside a readSet are correct.
     */
    public boolean equalHash(List readSet);

}
