import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P2 {
    // refactor
    // to do ...
    // constant
    // the size of lookUp table
    public final static int SIZE_LUT = 65536;

    // nets map to store all the hosts' information
    public static LinkedHashMap<String, NetsEntry> netsMap = null;
    // tuple space to store all the tuples
    public static ConcurrentHashMap<String, List<TupleSpaceEntry>> tupleSpace = new ConcurrentHashMap<>();
    // backup tuple space to store all the backup tuples
    public static ConcurrentHashMap<String, List<TupleSpaceEntry>> backUpTupleSpace = new ConcurrentHashMap<>();

    // lookUp table
    public static List<String> LUT = Collections.synchronizedList(new ArrayList<>());
    // reversed lookUp table
    public static Map<String, List<Integer>> RLUT = new ConcurrentHashMap<>();

    // the local directory for nets map, tuple space and backup tuple space
    public static String netsMapDir;
    public static String tupleSpaceDir;
    public static String backUpTupleSpaceDir;

    // the local host information
    public static String hostName = null;
    public static String ipAddr = null;
    public static int portNum = -1;

    /**
     * initialize lookUp table and reversed lookUp table
     * @return boolean
     */
    private boolean initLUTs() {
        if (P2.hostName != null) {
            List<Integer> listOfSlotID = new ArrayList<>();

            for (int i = 0; i < SIZE_LUT; i++) {
                P2.LUT.add(P2.hostName);
                listOfSlotID.add(i);
            }

            P2.RLUT.put(P2.hostName, listOfSlotID);
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    // suppress compiler warning when hashmap assignment
    public static void main(String args[]) {
        // create a instance of P2
        P2 p2 = new P2();

        // assign host name
        // check the user input
        if (args == null || args.length != 1) {
            System.out.println("Error: invalid parameter");
            System.out.println("Help: please type the following command to start the program");
            System.out.println("java P2 hostName");
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
        netsMap = new LinkedHashMap<>();

        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDirStr = "/tmp/szhang/linda/" + hostName + "/";
        String netsFileStr = "nets.txt";
        String tuplesFileStr = "tuples.txt";
        P2.netsMapDir = infoDirStr + netsFileStr;
        P2.tupleSpaceDir = infoDirStr + tuplesFileStr;

        // clean up the nets.txt and tuples.txt
        Path netsPath = Paths.get(P2.netsMapDir);
        Path tuplesPath = Paths.get(P2.tupleSpaceDir);
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
            System.out.println("P2: successfully changed the directory " + userNameDir + " to 777");
        } else {
            System.out.println("P2: failed to change the directory " + userNameDir + " to 777");
        }

        // change the linda level directory mode
        String lindaDirStr = "/tmp/szhang/linda/";
        File lindaDir = new File(lindaDirStr);
        if (lindaDir.setReadable(true, false) && lindaDir.setWritable(true, false) && lindaDir.setExecutable(true, false)) {
            System.out.println("P2: successfully changed the directory " + lindaDirStr + " to 777");
        } else {
            System.out.println("P2: failed to change the directory " + lindaDirStr + " to 777");
        }

        // change the hostName level directory mode
        if (infoDir.setReadable(true,false) && infoDir.setWritable(true,false) && infoDir.setExecutable(true,false)) {
            System.out.println("P2: successfully changed the directory " + infoDirStr + " to 777");
        } else {
            System.out.println("P2: failed to changed the directory " + infoDirStr + " to 777");
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
            System.out.println("P2: successfully changed the file " + netsFileStr + " to 666");
        } else {
            System.out.println("P2: failed change the file " + netsFileStr + " to 666");
        }

        if (tuplesFile.setReadable(true, false) && tuplesFile.setWritable(true, false)) {
            System.out.println("P2: successfully changed the file " + tuplesFileStr + " to 666");
        } else {
            System.out.println("P2: failed to change the file " + tuplesFileStr + " to 666");
        }

//        // store a empty tuple space into the file
//        try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.tupleSpaceDir))) {
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


        // initialize lookUp table and reversed lookUp table
        if (!p2.initLUTs()) {
            System.out.println("Error: failed to initialize the lookUp table");
        }

        // create the client in the same thread with P2
        // client handle user's input from the console
        Client client = new Client();
        client.setUp();

    }
}
