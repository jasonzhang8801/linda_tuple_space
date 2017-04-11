import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/10/17.
 */
public class Message implements Serializable {
    String command = null;
    ConcurrentHashMap netsMap = null;
    List<Object> tuple = null;
}
