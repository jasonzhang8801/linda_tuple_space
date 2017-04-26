import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/10/17.
 */
public class Message implements Serializable {
    // control field
    String command = null;
    boolean success = false;

    // network information
    String ipAddr = null;
    int portNum;

    // data field
    LinkedHashMap netsMap = null;
    List<Object> tuple = null;

}
