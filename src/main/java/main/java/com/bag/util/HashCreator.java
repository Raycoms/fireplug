package main.java.com.bag.util;

import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static main.java.com.bag.util.Constants.TAG_PRE;
import static main.java.com.bag.util.Constants.TAG_SNAPSHOT_ID;
import static main.java.com.bag.util.Constants.TAG_VERSION;

/**
 * Class used to create a Hash out of a node or relationship.
 */
public class HashCreator
{
    /**
     * Private constructor to hide implicit one.
     */
    private HashCreator()
    {
        /**
         * Intentionally left empty.
         */
    }
    
    /**
     * Create a sha1 hash-sum from a @NodeStorage.
     * @param node the input @NodeStorage
     * @return the return string (hash-sum)
     * @throws NoSuchAlgorithmException possible exception.
     */
    public static String sha1FromNode(final NodeStorage node) throws NoSuchAlgorithmException
    {
        final NodeStorage copy = new NodeStorage(node);
        copy.removeProperty(TAG_VERSION);
        copy.removeProperty(TAG_PRE);
        copy.removeProperty(TAG_SNAPSHOT_ID);

        final MessageDigest mDigest = MessageDigest.getInstance("SHA1");

        final byte[] result = mDigest.digest(copy.getBytes());

        final StringBuilder sb = new StringBuilder();
        for (final byte aResult : result)
        {
            sb.append(Integer.toString((aResult & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }

    /**
     * Create a sha1 hash-sum from a @RelationshipStorage.
     * @param relationShip the input @RelationshipStorage
     * @return the return string (hash-sum)
     * @throws NoSuchAlgorithmException possible exception.
     */
    public static String sha1FromRelationship(final RelationshipStorage relationShip) throws NoSuchAlgorithmException
    {
        final RelationshipStorage copy = new RelationshipStorage(relationShip);
        copy.removeProperty(TAG_VERSION);
        copy.removeProperty(TAG_PRE);
        copy.removeProperty(TAG_SNAPSHOT_ID);

        final MessageDigest mDigest = MessageDigest.getInstance("SHA1");

        final byte[] result = mDigest.digest(copy.getBytes());

        final StringBuilder sb = new StringBuilder();
        for (final byte aResult : result)
        {
            sb.append(Integer.toString((aResult & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }
}


