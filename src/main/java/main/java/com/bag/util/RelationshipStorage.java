package main.java.com.bag.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
     * The type of the relationship, optional may not be supported by various graph databases.
     */
    @Nullable
    private String type;

    /**
     * The properties of the relationship, may be empty as well.
     */
    @Nullable
    private HashMap<String, String> properties;

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
        this.type = type;
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id         string identifier of the node.
     * @param properties properties of the node.
     * @param startNode  node the relationship starts.
     * @param endNode    node the relationship ends.
     */
    public RelationshipStorage(@NotNull String id, @Nullable HashMap properties, @NotNull NodeStorage startNode, @NotNull NodeStorage endNode)
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
        this.type = type;
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

    @Nullable
    public String getType()
    {
        return type;
    }

    /**
     * Setter of the type.
     *
     * @param type new type.
     */
    public void setType(@Nullable final String type)
    {
        this.type = type;
    }

    /**
     * Getter of the properties.
     *
     * @return unmodifiable map of the properties.
     */
    @Nullable
    public Map<String, String> getProperties()
    {
        return properties == null ? null : Collections.unmodifiableMap(properties);
    }

    /**
     * Sets or adds new properties.
     *
     * @param properties a property map.
     */
    public void setProperties(@NotNull final HashMap<String, String> properties)
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
    public void addProperty(String description, String value)
    {
        if (this.properties == null)
        {
            this.properties = new HashMap<String, String>();
        }
        this.properties.put(description, value);
    }

    /**
     * Getter of the start node.
     * @return NodeStorage of start node.
     */
    @NotNull
    private NodeStorage getStartNode()
    {
        return startNode;
    }

    /**
     * Getter of the end node.
     * @return NodeStorage of end node.
     */
    @NotNull
    private NodeStorage getEndNode()
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
        if (getType() != null ? !getType().equals(that.getType()) : that.getType() != null)
        {
            return false;
        }
        if (getProperties() != null ? !getProperties().equals(that.getProperties()) : that.getProperties() != null)
        {
            return false;
        }
        return getStartNode().equals(that.getStartNode()) && getEndNode().equals(that.getEndNode());
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * (31 * (31 * getId().hashCode()
                + (getType() != null ? getType().hashCode() : 0))
                + (getProperties() != null ? getProperties().hashCode() : 0))
                + getStartNode().hashCode()) + getEndNode().hashCode();
    }
}
