import java.io.Serializable;

/**
 * Created by jasonzhang on 4/11/17.
 */


public enum TupleElementType implements Serializable {
    INTEGER("String"),
    FLOAT("Float"),
    STRING("String");

    private String type = null;

    private TupleElementType(String type) {
        this.type = type;
    }
}
