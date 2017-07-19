package main.java.com.bag.util;

import main.java.com.bag.util.storage.NodeStorage;
import main.java.com.bag.util.storage.RelationshipStorage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Class used to create a Hash out of a node or relationship.
 */
public class HashCreator
{
    /**
     * Create a sha1 hash-sum from a @NodeStorage.
     * @param node the input @NodeStorage
     * @return the return string (hash-sum)
     * @throws NoSuchAlgorithmException possible exception.
     */
    public static String sha1FromNode(NodeStorage node) throws NoSuchAlgorithmException
    {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");

        byte[] result = mDigest.digest(node.getBytes());

        final StringBuilder sb = new StringBuilder();
        for (final byte aResult : result)
        {
            sb.append(Integer.toString((aResult & 0xff) + 0x100, 16).substring(1));
        }

        Log.getLogger().warn(sb.toString());
        return sb.toString();
    }

    /**
     * Create a sha1 hash-sum from a @RelationshipStorage.
     * @param relationShip the input @RelationshipStorage
     * @return the return string (hash-sum)
     * @throws NoSuchAlgorithmException possible exception.
     */
    public static String sha1FromRelationship(RelationshipStorage relationShip) throws NoSuchAlgorithmException
    {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");

        byte[] result = mDigest.digest(relationShip.getBytes());

        StringBuilder sb = new StringBuilder();
        for (final byte aResult : result)
        {
            sb.append(Integer.toString((aResult & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }
}


