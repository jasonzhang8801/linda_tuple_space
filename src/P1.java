import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...
    public static ConcurrentHashMap<Integer, NetsEntry> netsMap = null;
//    public static ConcurrentHashMap<String, List>

    public static String ipAddr = null;
    public static int portNum = -1;

    @SuppressWarnings("unchecked")
    // suppress compiler warning when hashmap assignment
    public static void main(String args[]) {

        // assign host name
        // check the user input
        if (args == null || args.length != 1) {
            System.out.println("Error: invalid parameter");
            System.out.println("Help: please type the following command to start the program");
            System.out.println("java P1 hostName");
            return;
        }

        String hostName = args[0];

        // check if the host name is valid
        Pattern pattern = Pattern.compile("^[a-zA-Z][a-zA-Z0-9]*");
        Matcher matcher = pattern.matcher(hostName);
        if (!matcher.find()) {
            System.out.println("Error: invalid host name");
            System.out.println("Help: please review the following host naming convention");
            System.out.println("Host name must start with English letters");
            System.out.println("Host name only contains English letters and numbers");
            return;
        }

        // set up the server
        (new Thread(new Server(hostName))).start();

        // load the nets
        // to do ...
        netsMap = new ConcurrentHashMap<>();

        // create directories
        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDir = "/tmp/szhang/linda/" + hostName + "/";
        String netsFile = "nets.txt";
        String tuplesFile = "tuples.txt";
        new File(infoDir + netsFile).mkdirs();
        new File(infoDir + tuplesFile).mkdirs();

        // print host ip and port
        while (ipAddr == null || portNum == -1) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(ipAddr + " at port number: " + portNum);

        // create the client in the same thread with P1
        // client handle user's input from the console
        Client client = new Client();
        client.setUp();

    }
}
