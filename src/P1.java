import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...

    // nets map to store all the hosts' information
    public static ConcurrentHashMap<Integer, NetsEntry> netsMap = null;
    // tuple space to store all the tuples
    public static ConcurrentHashMap<String, List<TupleSpaceEntry>> tupleSpace = new ConcurrentHashMap<>();

    // the local directory for nets map and tuple space
    public static String netsMapDir;
    public static String tupleSpaceDir;

    // the local host information
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


        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDirStr = "/tmp/szhang/linda/" + hostName + "/";
        String netsFileStr = "nets.txt";
        String tuplesFileStr = "tuples.txt";
        P1.netsMapDir = infoDirStr + netsFileStr;
        P1.tupleSpaceDir = infoDirStr + tuplesFileStr;

        // clean up the nets.txt and tuples.txt
        Path netsPath = Paths.get(P1.netsMapDir);
        Path tuplesPath = Paths.get(P1.tupleSpaceDir);
        try {
            Files.deleteIfExists(netsPath);
            Files.deleteIfExists(tuplesPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // create directories
        File infoDir = new File(infoDirStr);
        infoDir.mkdirs();

        // change the directory's mode
        // change the userName level directory mode
        String userNameDirStr = "/tmp/szhang/";
        File userNameDir = new File(userNameDirStr);
        if (userNameDir.setReadable(true, false) && userNameDir.setWritable(true, false) && userNameDir.setExecutable(true, false)) {
            System.out.println("P1: successfully changed the directory " + userNameDir + " to 777");
        } else {
            System.out.println("P1: failed to change the directory " + userNameDir + " to 777");
        }

        // change the linda level directory mode
        String lindaDirStr = "/tmp/szhang/linda/";
        File lindaDir = new File(lindaDirStr);
        if (lindaDir.setReadable(true, false) && lindaDir.setWritable(true, false) && lindaDir.setExecutable(true, false)) {
            System.out.println("P1: successfully changed the directory " + lindaDirStr + " to 777");
        } else {
            System.out.println("P1: failed to change the directory " + lindaDirStr + " to 777");
        }

        // change the hostName level directory mode
        if (infoDir.setReadable(true,false) && infoDir.setWritable(true,false) && infoDir.setExecutable(true,false)) {
            System.out.println("P1: successfully changed the directory " + infoDirStr + " to 777");
        } else {
            System.out.println("P1: failed to changed the directory " + infoDirStr + " to 777");
        }

        // create files
        File netsFile = new File(netsMapDir);
        File tuplesFile = new File(tupleSpaceDir);
        try {
            netsFile.createNewFile();
            tuplesFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // change the files' mode
        if (netsFile.setReadable(true, false) && netsFile.setWritable(true, false)) {
            System.out.println("P1: successfully changed the file " + netsFileStr + " to 666");
        } else {
            System.out.println("P1: failed change the file " + netsFileStr + " to 666");
        }

        if (tuplesFile.setReadable(true, false) && tuplesFile.setWritable(true, false)) {
            System.out.println("P1: successfully changed the file " + tuplesFileStr + " to 666");
        } else {
            System.out.println("P1: failed to change the file " + tuplesFileStr + " to 666");
        }

//        // store a empty tuple space into the file
//        try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P1.tupleSpaceDir))) {
//            objOut.writeObject(new ConcurrentHashMap<String,List<TupleSpaceEntry>>());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

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
