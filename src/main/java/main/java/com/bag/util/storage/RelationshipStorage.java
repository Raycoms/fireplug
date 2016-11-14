package main.java.com.bag.util.storage;

import main.java.com.bag.util.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class used to store Relationships locally for a limited amount of time.
 */
public class RelationshipStorage implements Serializable
{
    /**
     * The string identifier of the relationship.
     */
    @NotNull
    private final String id;

    /**
     * The properties of the relationship, may be empty as well.
     */
    @Nullable
    private Map<String, Object> properties;

    /**
     * The node the relationship starts.
     */
    @NotNull
    private final NodeStorage startNode;

    /**
     * The node the relationship ends.
     */
    @NotNull
    private final NodeStorage endNode;

    public RelationshipStorage()
    {
        startNode = null;
        endNode = null;
        id = "";
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id        string identifier of the node.
     * @param startNode node the relationship starts.
     * @param endNode   node the relationship ends.
     */
    public RelationshipStorage(@NotNull String id, @NotNull NodeStorage startNode, @NotNull NodeStorage endNode)
    {
        this.id = id;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id        string identifier of the node.
     * @param type      type of the node.
     * @param startNode node the relationship starts.
     * @param endNode   node the relationship ends.
     */
    public RelationshipStorage(@NotNull String id, @Nullable String type, @NotNull NodeStorage startNode, @NotNull NodeStorage endNode)
    {
        this(id, startNode, endNode);
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id         string identifier of the node.
     * @param properties properties of the node.
     * @param startNode  node the relationship starts.
     * @param endNode    node the relationship ends.
     */
    public RelationshipStorage(@NotNull String id, @Nullable Map<String, Object> properties, @NotNull NodeStorage startNode, @NotNull NodeStorage endNode)
    {
        this(id, startNode, endNode);
        this.properties = properties;
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id         string identifier of the node.
     * @param type       type of the node.
     * @param properties properties of the node.
     * @param startNode  node the relationship starts.
     * @param endNode    node the relationship ends.
     */
    public RelationshipStorage(@NotNull String id, @Nullable String type, @Nullable HashMap properties, @NotNull NodeStorage startNode, @NotNull NodeStorage endNode)
    {
        this(id, startNode, endNode);
        this.properties = properties;
    }

    /**
     * Getter of the id.
     *
     * @return string description of the node.
     */
    public String getId()
    {
        return this.id;
    }

    /**
     * Getter of the properties.
     *
     * @return unmodifiable map of the properties.
     */
    @NotNull
    public Map<String, Object> getProperties()
    {
        return properties == null ? Collections.emptyMap() : new HashMap<>(properties);
    }

    /**
     * Sets or adds new properties.
     *
     * @param properties a property map.
     */
    public void setProperties(@NotNull final HashMap<String, Object> properties)
    {
        if (this.properties == null)
        {
            this.properties = properties;
        }
        else
        {
            this.properties.putAll(properties);
        }
    }

    /**
     * Add a new property to the properties.
     *
     * @param description description of the property.
     * @param value       value of the property.
     */
    public void addProperty(String description, Object value)
    {
        if (this.properties == null)
        {
            this.properties = new HashMap<>();
        }
        this.properties.put(description, value);
    }

    /**
     * Getter of the start node.
     * @return NodeStorage of start node.
     */
    @NotNull
    public NodeStorage getStartNode()
    {
        return startNode;
    }

    /**
     * Getter of the end node.
     * @return NodeStorage of end node.
     */
    @NotNull
    public NodeStorage getEndNode()
    {
        return endNode;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final RelationshipStorage that = (RelationshipStorage) o;

        if (!getId().equals(that.getId()))
        {
            return false;
        }

        if (!(this.getProperties().entrySet().containsAll(that.getProperties().entrySet())))
        {
            return false;
        }
        return getStartNode().equals(that.getStartNode()) && getEndNode().equals(that.getEndNode());
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * (31 * (31 * getId().hashCode()
                + getProperties().hashCode())
                + getStartNode().hashCode()) + getEndNode().hashCode());
    }

    /**
     * Returns a byte representation of the nodeStorage.
     * @return a byte array.
     */
    public byte[] getBytes()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(id);

        if (properties != null)
        {
            for(Map.Entry<String, Object> entry: properties.entrySet())
            {
                sb.append(entry.getKey()).append(entry.getValue());
            }
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        try
        {
            outputStream.write( sb.toString().getBytes() );
            outputStream.write( startNode.getBytes() );
            outputStream.write( endNode.getBytes() );

        }
        catch (IOException e)
        {
            Log.getLogger().info(e.getMessage());
        }

        return outputStream.toByteArray();
    }
}
