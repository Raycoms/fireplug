package main.java.com.bag.operations;

import bftsmart.reconfiguration.util.RSAKeyLoader;
import main.java.com.bag.server.database.interfaces.IDatabaseAccess;

/**
 * Generic command which may be sent to the database.
 */
@FunctionalInterface
public interface IOperation
{
    /**
     * Applies an operation to the database.
     * @param access Database access.
     * @param snapshotId SnapshotId.
     * @param keyLoader the key loader.
     * @param idClient the client id.
     */
    abstract void apply(final IDatabaseAccess access, long snapshotId, final RSAKeyLoader keyLoader, final int idClient);

    @Override
    abstract boolean equals(Object obj);

    @Override
    abstract String toString();
}
