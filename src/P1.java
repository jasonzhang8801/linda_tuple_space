import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...
    public static ConcurrentHashMap<Integer, NetsEntry> netsMap = null;

    @SuppressWarnings("unchecked")
    // suppress compiler warning when hashmap assignment
    public static void main(String args[]) {

        // assign host name
        // to do ...
        // check the user input
        if (args == null || args.length <= 0) {
            System.out.println("Error: invalid parameter");
        }

        String hostName = args[0];

        // create directories
        // to do ...
        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDir = "/tmp/szhang/linda/" + hostName + "/";
        String netsFile = "nets.txt";
        String tuplesFile = "tuples.txt";
        new File(infoDir + netsFile).mkdirs();
        new File(infoDir + tuplesFile).mkdirs();

        // load the nets
        // to do ...
        netsMap = new ConcurrentHashMap<>();

        // set up the server
        (new Thread(new Server(hostName))).start();
        System.out.println("The server named " + hostName + " started ...");


        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));)
        {
            String state = "idle";
            String[] tokens = null;

            while(true) {
                if (state == null) {
                    System.out.println("Error: state shouldn't be null");
                }

                switch(state.toLowerCase()) {
                    case "idle":
                        // read user input
                        System.out.print("linda> ");
                        String input = br.readLine();

                        // check the input
                        // to do ...
                        // CommandParser
                        // to do ...
                        tokens = input.split("\\s");
                        state = tokens[0];
                        System.out.println("I am idling...");
                        break;
                    case "add":
                        // construct the list of host with ip and port
                        // to do ...
                        List<String> listOfHost = new ArrayList<>();
                        String remoteIpAddr1 = tokens[1];
                        String remotePortNum1 = tokens[2];
                        String remoteIpAddr2 = tokens[3];
                        String remotePortNum2 = tokens[4];
                        listOfHost.add(remoteIpAddr1);
                        listOfHost.add(remotePortNum1);
                        listOfHost.add(remoteIpAddr2);
                        listOfHost.add(remotePortNum2);

                        // check if the input ip and port is valid
                        // to do ...

                        // num of host
                        int numOfHost = listOfHost.size() / 2;
                        int count = 0;

                        // construct the NetsEntry list
                        List<ConcurrentHashMap<Integer, NetsEntry>> listOfReceivedNetsMap = new ArrayList<>();

                        // collect the netsMap from remote server
                        // to do ...
                        // check how many netsMap the local server received
                        // set timeout if the local server doesn't receive anything for a given time
                        while (count <  listOfHost.size()) {
                            String remoteIpAddr = listOfHost.get(count++);
                            int remotePortNum = Integer.parseInt(listOfHost.get(count++));

                            try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                            {

                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                                )
                                {
                                    // construct the send message with add
                                    Message sendMessage = new Message();
                                    sendMessage.command = "add";

                                    // send the message to remote server
                                    out.writeObject(sendMessage);

                                    // construct the received object
                                    Message receivedMessage = null;

                                    try {
                                        if ((receivedMessage = (Message) in.readObject()) != null) {
                                            ConcurrentHashMap<Integer, NetsEntry> remoteNetsMap = receivedMessage.netsMap;
                                            if (receivedMessage.success != true) {
                                                System.out.println("Error: remoted server failed to send back its nets infomation");
                                            } else if (receivedMessage.command.equals("add")) {
                                                listOfReceivedNetsMap.add(remoteNetsMap);
                                                System.out.println("Client: recevied message " + receivedMessage.command);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }

                        // merge the local netsMap with netsMap from the remote servers
                        // check if received all nets info from remote servers
                        if (numOfHost != listOfReceivedNetsMap.size()) {
                            System.out.println("Error: expected to connect with " + numOfHost + " remote hosts, but only connected with " + listOfReceivedNetsMap.size());
                        } else {
                            // assign id to each remote host except local host
                            // assign id 0 to local host as master
                            for (int i = 0; i < numOfHost; i++) {
                                int remoteId = i + 1;
                                NetsEntry remoteNetsEntry = listOfReceivedNetsMap.get(i).get(0);
                                remoteNetsEntry.hostId = remoteId;
                                netsMap.put(remoteId, remoteNetsEntry);
                            }

                            // test only
                            // print out the nets map
//                            for (int id : P1.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println("key: " + id + " hostName: " + P1.netsMap.get(id).hostName + " hostId: " + P1.netsMap.get(id).hostId);
//                            }

                            // send the merged nets back to remote servers
                            for (int id : netsMap.keySet()) {
                                if (id != 0) {
                                    String remoteIpAddr = netsMap.get(id).ipAddr;
                                    int remotePortNum = netsMap.get(id).portNum;

                                    try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                                    {

                                        try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                                        )
                                        {
                                            // construct the merge message with add
                                            Message sendMessage = new Message();
                                            sendMessage.command = "merge";
                                            sendMessage.netsMap = P1.netsMap;

                                            // send the message to remote server
                                            out.writeObject(sendMessage);

                                            // construct the received object
                                            Message receivedMessage = null;

                                            try {
                                                if ((receivedMessage = (Message) in.readObject()) != null) {
                                                    if (receivedMessage.command.equals("merge")  && receivedMessage.success == true) {
                                                        System.out.println("Server: successfully add the host with IP: " + remoteIpAddr
                                                                + " port: " + remotePortNum );
                                                    } else {
                                                        System.out.println("Error: failed to add host with IP: " + remoteIpAddr
                                                                + " port: " + remotePortNum );
                                                    }
                                                }
                                            } catch (ClassNotFoundException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }

                        state = "idle";
                        break;
                    case "delete":
                        // do something
                        state = "idle";
                        break;
                    case "in":
                        // do something
                        state = "idle";
                        break;
                    case "out":
                        // do something
                        state = "idle";
                        break;
                    case "rd":
                        // do something
                        state = "idle";
                        break;
                    case "exit":
                        System.out.println("Exit linda!");
                        System.exit(0);
                        break;
                    default:
                        // do something
                        state = "idle";
                        System.out.println("Error: invalid command");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
