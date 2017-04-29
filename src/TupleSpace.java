import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/27/17.
 */
public class TupleSpace implements Serializable {
    ConcurrentHashMap<Integer, List<TupleSpaceEntry>> tupleSpace = null;
    ConcurrentHashMap<Integer, List<TupleSpaceEntry>> backUpTupleSpace = null;
}

class TupleSpaceEntry implements Serializable {
    // the number of duplicate tuple
    int count;
    // the tuple
    List<Object> tuple = null;
}

class Tuple implements Serializable {}
