package main.java.com.bag.server.database;

import com.sparsity.sparksee.gdb.*;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.storage.NodeStorage;

import static com.sparsity.sparksee.gdb.DataType.*;
import static com.sparsity.sparksee.gdb.DataType.String;

/**
 * Class containing some SparkSeeUtils to reduce the complexity of the @SparkSeeDatabaseAccess.java.
 */
public class SparkseeUtils
{
    /**
     * Private standard constructor to hide the implicit one.
     */
    private SparkseeUtils()
    {
        /*
         * Intentionally left empty.
         */
    }

    /**
     * Gets the object from the value.
     * @param value the input value.
     * @return an object from the value depending on the type.
     */
    protected static Object getObjectFromValue(Value value)
    {
        switch (value.getDataType())
        {
            case Boolean:
                return value.getBoolean();
            case Integer:
                return value.getInteger();
            case Long:
                return value.getLong();
            case Double:
                return value.getDouble();
            default:
                return value.getString();
        }
    }

    /**
     * Returns a Value object for a certain object.
     * @param obj ingoing object.
     * @return outgoing value.
     */
    protected static Value getValue(Object obj)
    {
        Value v = new Value();

        switch(getDataTypeFromObject(obj))
        {
            case Boolean:
                return v.setBoolean((Boolean) obj);
            case Integer:
                return v.setInteger((java.lang.Integer) obj);
            case Long:
                return v.setLong((java.lang.Long) obj);
            case Double:
                return v.setDouble((java.lang.Double) obj);
            default:
                return v.setString((java.lang.String) obj);
        }
    }

    /**
     * Gets a dataType from a certain object.
     * @param obj the ingoing object.
     * @return outgoing dataType, default a string.
     */
    protected static DataType getDataTypeFromObject(Object obj)
    {
        if(obj instanceof Integer)
        {
            return Integer;
        }
        else if(obj instanceof Long)
        {
            return Long;
        }
        else if(obj instanceof Boolean)
        {
            return Boolean;
        }
        else if (obj instanceof Double)
        {
            return Double;
        }

        return String;
    }

    /**
     * Tries to find a nodeType, if not able, creates it.
     * @param storage node of type.
     * @param graph graph object.
     * @return id of the type.
     */
    protected static int createOrFindNodeType(NodeStorage storage, Graph graph)
    {
        int nodeTypeId = graph.findType(storage.getId());
        if (Type.InvalidType == nodeTypeId)
        {
            return graph.newNodeType(storage.getId());
        }
        return nodeTypeId;
    }

    /**
     * Tries to find an attributeType, if not able, creates it.
     * @param key attributeKey.
     * @param value attributeValue.
     * @param nodeTypeId id of the nodeType.
     * @param graph graph object.
     * @return id of the type.
     */
    public static int createOrFindAttributeType(String key, Object value, int nodeTypeId, Graph graph)
    {
        int attributeId = graph.findAttribute(nodeTypeId, key);
        if (Attribute.InvalidAttribute == attributeId)
        {
            attributeId = graph.newAttribute(Type.getGlobalType(), key, SparkseeUtils.getDataTypeFromObject(value), AttributeKind.Indexed);
        }
        return attributeId;
    }


}
