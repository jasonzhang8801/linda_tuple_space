import java.util.List;

/**
 * Created by jasonzhang on 4/13/17.
 */
public class ParserEntry {
    // command name
    String commandName = null;

    // for add command
    // store remote host information
    List<String> remoteHostsInfo = null;

    // for non-add command
    // e.g. out, rd, in
    // store the tuple
    List<Object> tuple = null;
}
