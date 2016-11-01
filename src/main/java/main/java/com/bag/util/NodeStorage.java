package main.java.com.bag.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    @Nullable
    private Map<String, Object> properties;

    public NodeStorage()
    {
        id = "";
    }

    /**
     * Simple nodeStorage constructor.
     * @param id string identifier of the node.
     */
    public NodeStorage(@NotNull String id)
    {
        this.id = id;
    }

    /**
     * Simple nodeStorage constructor.
     * @param id string identifier of the node.
     * @param properties properties of the node.
     */
    public NodeStorage(@NotNull String id, @Nullable Map properties)
    {
        this.id = id;
        this.properties = properties;
    }

    /**
     * Simple nodeStorage constructor.
     * @param id string identifier of the node.
     * @param properties properties of the node.
     */
    public NodeStorage(@NotNull String id, @Nullable HashMap properties)
    {
        this.id = id;
        this.properties = properties;
    }

    /**
     * Getter of the id.
     * @return string description of the node.
     */
    public String getId()
    {
        return this.id;
    }

    /**
     * Getter of the properties.
     * @return unmodifiable map of the properties.
     */
    @NotNull
    public Map<String, Object> getProperties()
    {
        return properties == null ? Collections.emptyMap() : Collections.unmodifiableMap(properties);
    }

    /**
     * Sets or adds new properties.
     * @param properties a property map.
     */
    public void setProperties(@NotNull final HashMap<String, Object> properties)
    {
        if(this.properties == null)
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
     * @param description description of the property.
     * @param value value of the property.
     */
    public void addProperty(String description, Object value)
    {
        if(this.properties == null)
        {
            this.properties = new HashMap<>();
        }
        this.properties.put(description, value);
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
        if (!this.getId().equals(that.getId()))
        {
            return false;
        }
        return this.getProperties().entrySet().containsAll(that.getProperties().entrySet());
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * getId().hashCode() + getProperties().hashCode());
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
        return sb.toString().getBytes();
    }
}
