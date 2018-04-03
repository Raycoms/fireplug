package main.java.com.bag.util.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map;

import static main.java.com.bag.util.Constants.TAG_HASH;
import static main.java.com.bag.util.Constants.TAG_SNAPSHOT_ID;
import static main.java.com.bag.util.Constants.TAG_VERSION;

/**
 * Class used to store Nodes locally for a limited amount of time.
 */
public class NodeStorage implements Serializable
{
    /**
     * The string identifier of the node.
     */
    @NotNull
    private final String id;

    /**
     * The properties of the node, may be empty as well.
     */
    @NotNull
    private Map<String, Object> properties = new TreeMap<>();

    /**
     * Default constructor, needed for Kryo.
     */
    public NodeStorage()
    {
        id = "Node";
    }

    /**
     * Dummy constructor, this will create a dummy storage.
     * @param dummy true if dummy, false if dummy.
     */
    public NodeStorage(final boolean dummy)
    {
        id = "Dummy";
    }

    /**
     * NodeStorage copy constructor.
     *
     * @param copy the storage to copy.
     */
    public NodeStorage(@NotNull final NodeStorage copy)
    {
        this.id = copy.getId();
        this.properties.putAll(copy.getProperties());
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id string identifier of the node.
     */
    public NodeStorage(@NotNull String id)
    {
        this.id = "Node";
        this.properties = new TreeMap<>();
        this.properties.put("idx", id);
    }

    /**
     * Simple nodeStorage constructor.
     *
     * @param id         string identifier of the node.
     * @param properties properties of the node.
     */
    public NodeStorage(@NotNull String id, @NotNull Map<String, Object> properties)
    {
        this.id = "Node";
        this.properties.putAll(properties);
        if (!properties.containsKey("idx"))
        {
            this.properties.put("idx", id);
        }
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
     * Getter for a certain property from the properties.
     *
     * @param key the key of the property which should be get.
     */
    public Object getProperty(final String key)
    {
        return properties.get(key);
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

        final NodeStorage that = (NodeStorage) o;

        //We always have some type of type/class/name
        if (!this.getId().equals(that.getId()) && !this.getId().isEmpty() && !that.getId().isEmpty())
        {
            return false;
        }

        final Map<String, Object> thatMap = that.getProperties();
        final Map<String, Object> thisMap = this.getProperties();

        return thisMap.entrySet().containsAll(thatMap.entrySet())
                || thatMap.entrySet().containsAll(thisMap.entrySet());
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * getId().hashCode() + getProperties().hashCode());
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
     * Remove a certain property from the properties.
     *
     * @param key the key of the property which should be removed.
     */
    public void removeProperty(String key)
    {
        properties.remove(key);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(id);

        sb.append("[");
        for (Map.Entry<String, Object> item : properties.entrySet())
        {
            /*if(item.getKey().equals(TAG_HASH) || item.getKey().equals(TAG_SNAPSHOT_ID) || item.getKey().equals(TAG_VERSION))
            {
                continue;
            }*/
            sb.append(item.getKey());
            sb.append("=");
            sb.append(item.getValue());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");


        return sb.toString();
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

        return sb.toString().getBytes();
    }
}
