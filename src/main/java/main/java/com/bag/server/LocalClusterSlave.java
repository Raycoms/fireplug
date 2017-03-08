package main.java.com.bag.server;

import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class LocalClusterSlave extends AbstractRecoverable
{
    private static final String localConfig = "local";

    private boolean isPrimary = false;

    public LocalClusterSlave(final int id, final String instance, @NotNull final ServerWrapper wrapper)
    {
        super(id, instance, localConfig);
    }

    //todo send commit to primary

    //todo store if is primary and if just became primary try to register with global cluster

    //todo for this fill in the Wrapper with the global cluster instance
}
