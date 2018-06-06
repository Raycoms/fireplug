package com.bag.util.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bag.util.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.TreeMap;
import java.util.Map;

import static com.bag.util.Constants.TAG_HASH;
import static com.bag.util.Constants.TAG_SNAPSHOT_ID;
import static com.bag.util.Constants.TAG_VERSION;

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
    @NotNull
    private Map<String, Object> properties = new TreeMap<>();

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
        startNode = new NodeStorage();
        endNode = new NodeStorage();
        id = "";
    }

    /**
     * RelationshipStorage copy constructor.
     *
     * @param copy the storage to copy.
     */
    public RelationshipStorage(@NotNull final RelationshipStorage copy)
    {
        this.id = copy.getId();
        this.startNode = copy.startNode;
        this.endNode = copy.endNode;
        this.properties.putAll(copy.getProperties());
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id        string identifier of the node.
     * @param startNode node the relationship starts.
     * @param endNode   node the relationship ends.
     */
    public RelationshipStorage(@NotNull final String id, @NotNull final NodeStorage startNode, @NotNull final NodeStorage endNode)
    {
        this.id = id;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id         string identifier of the node.
     * @param properties properties of the node.
     * @param startNode  node the relationship starts.
     * @param endNode    node the relationship ends.
     */
    public RelationshipStorage(@NotNull final String id, @Nullable final Map<String, Object> properties, @NotNull final NodeStorage startNode, @NotNull final NodeStorage endNode)
    {
        this(id, startNode, endNode);
        this.properties.putAll(properties);
    }

    /**
     * Getter of the id.
     *
     * @return string description of the node.
     */
    @NotNull
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
        return new TreeMap<>(properties);
    }

    /**
     * Sets or adds new properties.
     *
     * @param properties a property map.
     */
    public void setProperties(@NotNull final Map<String, Object> properties)
    {
        this.properties.putAll(properties);
    }

    /**
     * Add a new property to the properties.
     *
     * @param description description of the property.
     * @param value       value of the property.
     */
    public void addProperty(String description, Object value)
    {
        this.properties.put(description, value);
    }

    /**
     * Getter of the start node.
     *
     * @return NodeStorage of start node.
     */
    @NotNull
    public NodeStorage getStartNode()
    {
        return startNode;
    }

    /**
     * Getter of the end node.
     *
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

        if (!this.getId().equals(that.getId()) && !this.getId().isEmpty() && !that.getId().isEmpty())
        {
            return false;
        }

        final Map<String, Object> thatMap = that.getProperties();
        final Map<String, Object> thisMap = this.getProperties();

        if(!thisMap.entrySet().containsAll(thatMap.entrySet())
                && !thatMap.entrySet().containsAll(thisMap.entrySet()))
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("(");

        sb.append(startNode.toString());
        sb.append(") <-- ");
        sb.append(id);

        sb.append("[");
        for (Map.Entry<String, Object> item : properties.entrySet())
        {
            if(item.getKey().equals(TAG_HASH) || item.getKey().equals(TAG_SNAPSHOT_ID) || item.getKey().equals(TAG_VERSION))
            {
                continue;
            }
            sb.append(item.getKey());
            sb.append("=");
            sb.append(item.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("] --> (");
        sb.append(endNode.toString());

        sb.append(")");
        return sb.toString();
    }

    /**
     * Remove a certain property from the properties.
     *
     * @param key the key of the property which should be removed.
     */
    public void removeProperty(final String key)
    {
        properties.remove(key);
    }

    /**
     * Getter for a certain property from the properties.
     *
     * @param key the key of the property which should be get.
     */
    public Object getProperty(final String key)
    {
        return properties.get(key);
    }

    /**
     * Get the kryo byte array of a relationship storage.
     * @param kryo the kryo object.
     * @return the byte array.
     */
    public byte[] getByteArray(final Kryo kryo)
    {
        final Output output = new Output(0, 100024);
        kryo.writeObject(output, this);
        byte[] bytes = output.getBuffer();
        output.close();
        return bytes;
    }

    /**
     * Get the storage class from a byte array.
     * @param kryo the kryo object.
     * @param input the byte array.
     * @return the storage.
     */
    public RelationshipStorage fromBytes(final Kryo kryo, final byte[] input)
    {
        final Input tempInput = new Input(input);
        return kryo.readObject(tempInput, RelationshipStorage.class);
    }

    /**
     * Returns a byte representation of the nodeStorage.
     *
     * @return a byte array.
     */
    public byte[] getBytes()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(id);

        for (Map.Entry<String, Object> entry : properties.entrySet())
        {
            sb.append(entry.getKey()).append(entry.getValue());
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try
        {
            outputStream.write(sb.toString().getBytes());
            outputStream.write(startNode.getBytes());
            outputStream.write(endNode.getBytes());
        }
        catch (IOException e)
        {
            Log.getLogger().info(e.getMessage(), e);
        }

        return outputStream.toByteArray();
    }
}
