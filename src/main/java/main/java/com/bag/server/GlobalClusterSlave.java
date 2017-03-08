package main.java.com.bag.server;

import org.jetbrains.annotations.NotNull;

/**
 * Class handling server communication in the global cluster.
 */
public class GlobalClusterSlave extends AbstractRecoverable
{
    private static final String globalConfig = "global";

    public GlobalClusterSlave(final int id, final String instance, @NotNull final ServerWrapper wrapper)
    {
        super(id, instance, globalConfig);
    }

    public void distributeCommit()
    {
        //todo if receives commit and global decision is commit -> notify slaves
        //todo -> we will need the Wrapper class instance to send it to the local cluster.
    }

    public void storeConfig()
    {

    }



    //todo need a way to register a new slave in the global cluster.
        //todo -> ask the majority to check if the local slave can register -> if they decide on yes change all the config files
            //todo -> check if old offline or if old is in accordance
}
