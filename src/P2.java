import java.io.*;
import java.net.Socket;
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
    // K: slot#, V: list of tuple
    public static ConcurrentHashMap<Integer, List<TupleSpaceEntry>> tupleSpace = new ConcurrentHashMap<>();
    // backup tuple space to store all the backup tuples
    // K: slot#, V: list of tuple
    public static ConcurrentHashMap<Integer, List<TupleSpaceEntry>> backUpTupleSpace = new ConcurrentHashMap<>();

    // lookUp table
    // index: slot#, val: hostName
    public static List<String> LUT = Collections.synchronizedList(new ArrayList<>());
    // reversed lookUp table
    // key: hostName, val: list of slot#
    public static Map<String, List<Integer>> RLUT = new ConcurrentHashMap<>();

    // the local directory for nets map, tuple space and backup tuple space
    public static String netsMapDir;
    public static String tupleSpaceDir;

    //
    private String infoDirStr = null;
    private String netsFileStr = null;
    private String tuplesFileStr = null;

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

    /**
     * initialize the directories and files, netsMap and tuples
     * @return
     */
    private void initFileSys() {
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
            System.out.println("System: successfully changed the directory " + userNameDir + " to 777");
        } else {
            System.out.println("System: failed to change the directory " + userNameDir + " to 777");
        }

        // change the linda level directory mode
        String lindaDirStr = "/tmp/szhang/linda/";
        File lindaDir = new File(lindaDirStr);
        if (lindaDir.setReadable(true, false) && lindaDir.setWritable(true, false) && lindaDir.setExecutable(true, false)) {
            System.out.println("System: successfully changed the directory " + lindaDirStr + " to 777");
        } else {
            System.out.println("System: failed to change the directory " + lindaDirStr + " to 777");
        }

        // change the hostName level directory mode
        if (infoDir.setReadable(true,false) && infoDir.setWritable(true,false) && infoDir.setExecutable(true,false)) {
            System.out.println("System: successfully changed the directory " + infoDirStr + " to 777");
        } else {
            System.out.println("System: failed to changed the directory " + infoDirStr + " to 777");
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
            System.out.println("System: successfully changed the file " + netsFileStr + " to 666");
        } else {
            System.out.println("System: failed change the file " + netsFileStr + " to 666");
        }

        if (tuplesFile.setReadable(true, false) && tuplesFile.setWritable(true, false)) {
            System.out.println("System: successfully changed the file " + tuplesFileStr + " to 666");
        } else {
            System.out.println("System: failed to change the file " + tuplesFileStr + " to 666");
        }
    }

    /**
     * after failure, the local host reboots and update other hosts' netsMap
     */
    private void updatePort() {
        // send the message to remote servers
        // only update netsMap
        // boolean flag to indicate if at least one remote host is alive in the cluster
        boolean isCluster = false;

        for (String hostName : P2.netsMap.keySet()) {

            // send the message to the remote hosts except current one
            if (!hostName.equals(P2.hostName)) {
                String remoteIpAddr = P2.netsMap.get(hostName).ipAddr;
                int remotePortNum = P2.netsMap.get(hostName).portNum;

                try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                {
                    try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                         ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    )
                    {
                        // construct the merge message with add
                        Message sendMessage = new Message();
                        sendMessage.command = "replace_nets";
                        sendMessage.success = false;
                        sendMessage.netsMap = P2.netsMap;

                        // send the message to remote server
                        out.writeObject(sendMessage);
                        System.out.println("Client: send the message with updated nets map to remote host with IP "
                                + remoteIpAddr);

                        // construct the received object
                        Message receivedMessage = null;
                        try {
                            if ((receivedMessage = (Message) in.readObject()) != null) {
                                if (receivedMessage.command.equals("replace_nets")  && receivedMessage.success) {
                                    System.out.println("Client: successfully update port number at the host with IP: " + remoteIpAddr
                                            + " port: " + remotePortNum );
                                } else {
                                    System.out.println("Error: failed to update port number at host with IP: " + remoteIpAddr
                                            + " port: " + remotePortNum );
                                }
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    // update the boolean flag
                    // there are still other hosts in the cluster
                    isCluster = true;

                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("System: failed to connect with the remote host with IP: " + remoteIpAddr
                            + " port: " + remotePortNum);
                }
            }
        }

        // no hosts left in the cluster
        if (!isCluster) {
            System.out.println("System: no other hosts in the cluster");

            // clean up the nets.txt and tuples.txt
            Path netsPath = Paths.get(P2.netsMapDir);
            Path tuplesPath = Paths.get(P2.tupleSpaceDir);
            try {
                Files.deleteIfExists(netsPath);
                Files.deleteIfExists(tuplesPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("System: clean up the local host file system ...");

            // exit linda system
            System.out.println("System: exiting ... ");
            System.out.println("System: please restart Linda again");
            System.exit(0);

        }

        // update the netsMap into the file
        try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.netsMapDir))) {
            objOut.writeObject(P2.netsMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDirStr = "/tmp/szhang/linda/" + hostName + "/";
        String netsFileStr = "nets.txt";
        String tuplesFileStr = "tuples.txt";
        p2.infoDirStr = infoDirStr;
        p2.netsFileStr = netsFileStr;
        p2.tuplesFileStr = tuplesFileStr;
        P2.netsMapDir = infoDirStr + netsFileStr;
        P2.tupleSpaceDir = infoDirStr + tuplesFileStr;

        // check if the nets file exist in disk
        File netsMapFile = new File(P2.netsMapDir);

        if (!netsMapFile.exists()) {
            // if netsMap file not exist
            System.out.println("System: never see this host before");
            // init netMaps in memory
            P2.netsMap = new LinkedHashMap<>();
            p2.initFileSys();
        } else {
            // if netsMap file exist
            System.out.println("System: saw this host before");

            // read the netsMap into the memory
            try (ObjectInputStream objIn = new ObjectInputStream(new FileInputStream(P2.netsMapDir))) {
                 P2.netsMap = (LinkedHashMap<String, NetsEntry>) objIn.readObject();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // set up the server
        (new Thread(new Server(hostName))).start();

        // print host ip and port
        while (P2.ipAddr == null || P2.portNum == -1) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(P2.ipAddr + " at port number: " + P2.portNum);

        // write netsMap back to the disk
        try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.netsMapDir))) {
            objOut.writeObject(P2.netsMap);
            System.out.println("System: write netsMap into disk");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // check the netsMap's size
        // when size > 1, the local host was in the cluster
        // when size == 1, the local host wasn't in the cluster before
        if (P2.netsMap.size() > 1) {
            System.out.println("System: the local host is in the cluster before");
            // LUT and RLUT don't change during failure
            // ask other hosts to update the latest port number
            p2.updatePort();

            // however, tuple space need to be updated
            // ASSUMPTION: during failure, no host join and leave

            // recover the original tuple space from remote backUp host
            String nextHostName = Utility.getNextHostName(P2.hostName, P2.netsMap);
            String nextHostIpAddr = P2.netsMap.get(nextHostName).ipAddr;
            int nextHostPortNum = P2.netsMap.get(nextHostName).portNum;

            try (Socket socket = new Socket(nextHostIpAddr, nextHostPortNum)) {

                try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream()) ) {

                    // construct the send message
                    Message sendMessage = new Message();
                    sendMessage.command = "req_ts";
                    sendMessage.oriOrBu = Const.BU;
                    sendMessage.tupleSpace = null;
                    sendMessage.success = false;

                    objOut.writeObject(sendMessage);

                    // construct the received message
                    Message receivedMessage = null;

                    if ((receivedMessage = (Message) objIn.readObject()) != null) {
                        if (receivedMessage.command.equals("req_ts")
                                && receivedMessage.success
                                && receivedMessage.tupleSpace != null) {
                            P2.tupleSpace = receivedMessage.tupleSpace;
                            System.out.println("System: recover the original tuple space from host with IP "
                                    + nextHostIpAddr);
                        } else {
                            System.out.println("System error: failed to recover the original tuple space from host with IP "
                                    + nextHostIpAddr);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            // recover the backUp tuple space from remote original host
            String preHostName = Utility.getPreHostName(P2.hostName, P2.netsMap);
            String preHostIpAddr = P2.netsMap.get(preHostName).ipAddr;
            int preHostPortNum = P2.netsMap.get(preHostName).portNum;

            try (Socket socket = new Socket(preHostIpAddr, preHostPortNum)) {

                try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream()) ) {

                    // construct the send message
                    Message sendMessage = new Message();
                    sendMessage.command = "req_ts";
                    sendMessage.oriOrBu = Const.ORI;
                    sendMessage.tupleSpace = null;
                    sendMessage.success = false;

                    objOut.writeObject(sendMessage);

                    // construct the received message
                    Message receivedMessage = null;

                    if ((receivedMessage = (Message) objIn.readObject()) != null) {
                        if (receivedMessage.command.equals("req_ts")
                                && receivedMessage.success
                                && receivedMessage.tupleSpace != null) {
                            P2.backUpTupleSpace = receivedMessage.tupleSpace;
                            System.out.println("System: recover the backUp tuple space from host with IP "
                                    + preHostIpAddr);
                        } else {
                            System.out.println("System error: failed to recover the backUp tuple space from host with IP "
                                    + preHostIpAddr);
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } else if (P2.netsMap.size() == 1) {
            System.out.println("System: the local host is not in the cluster before");

            // initialize lookUp table and reversed lookUp table
            if (!p2.initLUTs()) {
                System.out.println("Error: failed to initialize the lookUp table");
            }
        } else {
            System.out.println("System error: netsMap should >= 1");
        }

        // create the client in the same thread with P2
        // client handle user's input from the console
        Client client = new Client();
        client.setUp();

    }
}
