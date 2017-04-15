import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class Client {

    @SuppressWarnings("unchecked")
    public void setUp() {
        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));)
        {
            String state = "idle";
            ParserEntry parserEntry = null;
            int numOfHost = -1;

            while(true) {
                if (state == null) {
                    System.out.println("System error: state shouldn't be null");
                    return;
                }

                switch(state.toLowerCase()) {
                    case "idle":
                        // read user input
                        System.out.print("linda> ");
                        String input = br.readLine();

                        // check the input
                        Pattern pattern = Pattern.compile("^\\s*$");
                        Matcher matcher = pattern.matcher(input);
                        if (matcher.find()) {
                            System.out.println("Warning: please type the valid command");
                            System.out.println("Please type command \"help\" to get more details");
                            break;
                        }

                        // parse user's input
                        parserEntry = Utility.parser(input);

                        // wait and let user try again
                        if (parserEntry == null) {
                            break;
                        }

                        // change the state
                        state = parserEntry.commandName;

                        break;
                    case "add":
                        // construct the list of host with ip and port
                        List<String> listOfHost = parserEntry.remoteHostsInfo;

                        // num of host
                        numOfHost = listOfHost.size() / 2;
                        if (numOfHost <= 0) {
                            System.out.println("System error: the number of host should be larger than zero");
                            System.out.println("Help: please use command \"add\" to add more hosts");
                            state = "idle";
                            break;
                        }

                        int count = 0;

                        // construct the NetsEntry list
                        List<ConcurrentHashMap<Integer, NetsEntry>> listOfReceivedNetsMap = new ArrayList<>();

                        // collect the netsMap from remote server
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
                                    System.out.println("Client: send the message with command add to the remote host with IP "
                                            + remoteIpAddr);

                                    // construct the received object
                                    Message receivedMessage = null;

                                    try {
                                        if ((receivedMessage = (Message) in.readObject()) != null) {
                                            ConcurrentHashMap<Integer, NetsEntry> remoteNetsMap = receivedMessage.netsMap;
                                            if (receivedMessage.success != true) {
                                                System.out.println("Error: remoted server failed to send back its nets infomation");
                                            } else if (receivedMessage.command.equals("add")) {
                                                listOfReceivedNetsMap.add(remoteNetsMap);
                                                System.out.println("Client: received message with nets map from the remote host"
                                                        + remoteIpAddr);
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
                                P1.netsMap.put(remoteId, remoteNetsEntry);
                            }

                            System.out.println("Client: successfully merge the nets map");
                            // test only
                            // print out the nets map
//                            for (int id : P1.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println("key: " + id + " hostName: " + P1.netsMap.get(id).hostName + " hostId: " + P1.netsMap.get(id).hostId);
//                            }

                            // send the merged nets back to remote servers
                            for (int id : P1.netsMap.keySet()) {
                                if (id != 0) {
                                    String remoteIpAddr = P1.netsMap.get(id).ipAddr;
                                    int remotePortNum = P1.netsMap.get(id).portNum;

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
                                            System.out.println("Client: send back the message with updated nets map to remote host with IP "
                                                    + remoteIpAddr);

                                            // construct the received object
                                            Message receivedMessage = null;

                                            try {
                                                if ((receivedMessage = (Message) in.readObject()) != null) {
                                                    if (receivedMessage.command.equals("merge")  && receivedMessage.success == true) {
                                                        System.out.println("Client: successfully add the host with IP: " + remoteIpAddr
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
                        // construct the tuple
                        List<Object> tuple = parserEntry.tuple;

                        // check the number of host
                        if (P1.netsMap.size() <= 0) {
                            System.out.println("System error: the number of host should be larger than zero");
                            System.out.println("Help: please use command \"add\" to add more hosts");
                            state = "idle";
                            break;
                        }

                        // hash the tuple to get the host information
                        int hostId = Utility.hashToId(tuple, P1.netsMap.size());
//                        System.out.println("host id is " + hostId);
//                        System.out.println("host name is " + P1.netsMap.get(hostId).hostName
//                                + "host ip is " + P1.netsMap.get(hostId).ipAddr
//                                + "host port is " + P1.netsMap.get(hostId).portNum);
                        String remoteIpAddr = P1.netsMap.get(hostId).ipAddr;
                        int remotePortNum = P1.netsMap.get(hostId).portNum;

                        // connect to the remote host
                        try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                        {

                            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                            )
                            {
                                // construct the send message with out
                                Message sendMessage = new Message();
                                sendMessage.command = "out";
                                sendMessage.tuple = tuple;

                                // send the message to remote server
                                out.writeObject(sendMessage);
                                System.out.println("Client: send the tuple to the host with IP " + remoteIpAddr);

                                // construct the received object
                                Message receivedMessage = null;

                                try {
                                    if ((receivedMessage = (Message) in.readObject()) != null) {
                                        if (receivedMessage.success != true) {
                                            System.out.println("Error: remoted server failed to put the given tuple");
                                        } else if (receivedMessage.command.equals("out")) {
                                            System.out.println("put tuple " + receivedMessage.tuple.toString() + " on " + remoteIpAddr);
                                        }
                                    }
                                } catch (ClassNotFoundException e) {
                                    e.printStackTrace();
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


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
