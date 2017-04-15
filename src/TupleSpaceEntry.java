/**
 * Created by jasonzhang on 4/14/17.
 */
import java.io.Serializable;
import java.util.List;
/**
 * tupleSpace
 * is hash table where K is the tuple hash, V is linked list containing all the tuple with the same hash
 * tuple space entry
 * is the smallest unit in the tuple space in the linked list
 */
public class TupleSpaceEntry implements Serializable {
    // the number of duplicate tuple
    int count;

    // the tuple
    List<Object> tuple = null;
}
