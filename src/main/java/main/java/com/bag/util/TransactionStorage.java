package main.java.com.bag.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Used to store extra information of a transaction.
 */
public class TransactionStorage
{
    /**
     * Nodes the transactions read already.
     */
    private ArrayList<NodeStorage> readSetNodes;

    /**
     * Relationships the transaction read already.
     */
    private ArrayList<RelationshipStorage> readSetRelationships;

    public TransactionStorage()
    {
        readSetNodes = new ArrayList<>();
        readSetRelationships = new ArrayList<>();
    }

    /**
     * Fills the readSetNodes.
     * @param nodeStorageList nodes to add.
     */
    public void fillReadSetNodes(List<NodeStorage> nodeStorageList)
    {
        readSetNodes.addAll(nodeStorageList);
    }

    /**
     * Fills the readSetNodes.
     * @param relationshipStorageList nodes to add.
     */
    public void fillReadSetRelationships(List<RelationshipStorage> relationshipStorageList)
    {
        readSetRelationships.addAll(relationshipStorageList);
    }

    /**
     * Getter of the nodes readSet.
     * @return immutable list of the readSet of the nodes.
     */
    public List<NodeStorage> getReadSetNodes()
    {
        return Collections.unmodifiableList(readSetNodes);
    }

    /**
     * Getter of the relationship readSet.
     * @return immutable list of the readSet of the relationships.
     */
    public List<RelationshipStorage> getReadSetRelationships()
    {
        return Collections.unmodifiableList(readSetRelationships);
    }
}
