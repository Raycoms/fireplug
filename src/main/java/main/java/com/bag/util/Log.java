package main.java.com.bag.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Log
{
    public static final String BAG_DESC = "BAG";
    /**
     * Program logger
     */
    private static Logger logger = null;

    /**
     * Private constructor to hide the public one.
     */
    private Log()
    {
        //Hides implicit constructor.
    }

    /**
     * Getter for the minecolonies Logger.
     *
     * @return the logger.
     */
    public static Logger getLogger()
    {
        if (logger == null)
        {
            Log.logger = LogManager.getLogger(BAG_DESC);
        }
        return logger;
    }
}
