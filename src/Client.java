import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class Client {
    // broadcast control
    // boolean flag to indicate if there is implicit type match to broadcast all the host in the nets map
    public static boolean isBroadcast = false;
    // broadcast queue to store the received messages
    public static Deque<Message> broadcastQueue= new ArrayDeque<>();

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
                    case "idle": {
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
                    }
                    case "add": {
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
                        List<LinkedHashMap<String, NetsEntry>> listOfReceivedNetsMap = new ArrayList<>();

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
                                            LinkedHashMap<String, NetsEntry> remoteNetsMap = receivedMessage.netsMap;
                                            if (receivedMessage.success != true) {
                                                System.out.println("Error: remote server failed to send back its nets infomation");
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
                            } catch (Exception e) {
//                                e.printStackTrace();
                                System.out.println("Client: invalid IP or port number");
                                System.out.println("Help: please check the host IP and port number");
                                System.exit(0);
                            }

                        }

                        // merge the local netsMap with netsMap from the remote servers
                        // check if received all nets info from remote servers
                        if (numOfHost != listOfReceivedNetsMap.size()) {
                            System.out.println("Error: expected to connect with " + numOfHost + " remote hosts, but only connected with " + listOfReceivedNetsMap.size());
                        } else {
                            // no ID assigned
                            // K: hostName, V: netsEntry
                            for (int i = 0; i < numOfHost; i++) {
                                LinkedHashMap<String, NetsEntry> remoteNetsMap = listOfReceivedNetsMap.get(i);
                                String remoteHostName = Utility.netsMapIndexToKey(0, remoteNetsMap);
                                P2.netsMap.put(remoteHostName, remoteNetsMap.get(remoteHostName));
                            }

                            System.out.println("Client: successfully merge the nets map");

                            // test only
                            // print out the nets map
//                            int hostId = 0;
//                            for (String hostName : P2.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println(" hostName: " + hostName + " hostId: " + hostId++);
//                            }

                            // send the merged nets back to remote servers
                            for (String hostName : P2.netsMap.keySet()) {
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
                                            sendMessage.command = "merge";
                                            sendMessage.netsMap = P2.netsMap;

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
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }

                            // update the netsMap into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.netsMapDir))) {
                                objOut.writeObject(P2.netsMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        state = "idle";
                        break;
                    }
                    case "out": {
                        // construct the tuple
                        List<Object> tuple = parserEntry.tuple;

                        // check the number of host
                        if (P2.netsMap.size() <= 0) {
                            System.out.println("System error: the number of host should be larger than zero");
                            System.out.println("Help: please use command \"add\" to add more hosts");
                            state = "idle";
                            break;
                        }

                        // hash the tuple to get the host information
                        int hostId = Utility.hashToId(tuple, P2.netsMap.size());
                        // list of hostName in inserted order
                        List<String> listOfHostName = new ArrayList<>(P2.netsMap.keySet());
                        String remoteIpAddr = P2.netsMap.get(hostId).ipAddr;
                        int remotePortNum = P2.netsMap.get(hostId).portNum;

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
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        state = "idle";
                        break;
                    }
                    case "rd": {
                        // check the number of host
                        if (P2.netsMap.size() <= 0) {
                            System.out.println("System error: the number of host should be larger than zero");
                            System.out.println("Help: please use command \"add\" to add more hosts");
                            state = "idle";
                            break;
                        }

                        if (!Client.isBroadcast) {
                            // command "rd" with exact type match

                            // construct the tuple
                            List<Object> tuple = parserEntry.tuple;

                            // hash the tuple to get the host information
                            int hostId_rd = Utility.hashToId(tuple, P2.netsMap.size());
//                        System.out.println("host id is " + hostId);
//                        System.out.println("host name is " + P2.netsMap.get(hostId).hostName
//                                + "host ip is " + P2.netsMap.get(hostId).ipAddr
//                                + "host port is " + P2.netsMap.get(hostId).portNum);
                            String remoteIpAddr = P2.netsMap.get(hostId_rd).ipAddr;
                            int remotePortNum = P2.netsMap.get(hostId_rd).portNum;

                            // connect to the remote host
                            try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                            {

                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                                )
                                {
                                    // construct the send message with out
                                    Message sendMessage = new Message();
                                    sendMessage.command = "rd";
                                    sendMessage.tuple = tuple;

                                    // send the message to remote server
                                    out.writeObject(sendMessage);
                                    System.out.println("Client: send \"rd\" message with the tuple "
                                            + tuple.toString()
                                            + " to the host with IP " + remoteIpAddr);

                                    // construct the received object
                                    Message receivedMessage = null;

                                    try {
                                        if ((receivedMessage = (Message) in.readObject()) != null) {
                                            if (receivedMessage.success != true) {
                                                System.out.println("Error: remoted server failed to read the given tuple");
                                            } else if (receivedMessage.command.equals("rd")) {
                                                System.out.println("Client: read tuple " + receivedMessage.tuple.toString() + " on " + remoteIpAddr);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            // broadcast all the host
                            System.out.println("Client: implicit type match query");
                            System.out.println("Client: broadcast all the hosts in the nets map");

                            // construct the broadcast message with command "rd_broadcast"
                            Message sendMessage = new Message();
                            sendMessage.command = "rd_broadcast";
                            sendMessage.tuple = parserEntry.tuple;

                            // thread pool
                            List<Thread> threadPool = new ArrayList<>();

                            // send out the broadcast messages
                            for (NetsEntry netsEntry : P2.netsMap.values()) {
                                String remoteIpAddr = netsEntry.ipAddr;
                                int remotePortNum = netsEntry.portNum;

                                Thread broadcastThread = new Thread(new ClientWorker("rd_broadcast",
                                        parserEntry.tuple, remoteIpAddr, remotePortNum));
                                broadcastThread.start();
                                threadPool.add(broadcastThread);
                                System.out.println("Client: start broadcast thread with id: " + broadcastThread.getId());
                            }

                            // received message
                            Message receivedMessage = null;

                            // keep checking the broadcast message queue
                            while (receivedMessage == null) {
                                // NEED TO FIX
                                // to do ...
                                // if user A uses the client to do the implicit type match first, e.g. rd(?i:int)
                                // then another user B uses the client to put out(1) into the tuple space
                                // user A's client still gets stuck unless user A's console keeps printing stuff...
                                System.out.print("");
                                if (!Client.broadcastQueue.isEmpty()) {
                                    receivedMessage = Client.broadcastQueue.peek();
                                }
                            }

                            // terminate all the broadcast threads in the thread pool
                            for (Thread thread : threadPool) {
                                System.out.println("System: clean broadcast thread with id: " + thread.getId());
                                thread.interrupt();
                            }

                            System.out.println("read tuple " + receivedMessage.tuple.toString()
                                    + " on " + receivedMessage.ipAddr);

                            // turn off broadcast mode
                            Client.isBroadcast = false;
                            Client.broadcastQueue.clear();
                        }

                        state = "idle";
                        break;
                    }
                    case "in": {
                        // check the number of host
                        if (P2.netsMap.size() <= 0) {
                            System.out.println("System error: the number of host should be larger than zero");
                            System.out.println("Help: please use command \"add\" to add more hosts");
                            state = "idle";
                            break;
                        }

                        if (!Client.isBroadcast) {
                            // command "in" with exact type match

                            // construct the tuple
                            List<Object> tuple = parserEntry.tuple;

                            // hash the tuple to get the host information
                            int hostId_rd = Utility.hashToId(tuple, P2.netsMap.size());
//                        System.out.println("host id is " + hostId);
//                        System.out.println("host name is " + P2.netsMap.get(hostId).hostName
//                                + "host ip is " + P2.netsMap.get(hostId).ipAddr
//                                + "host port is " + P2.netsMap.get(hostId).portNum);
                            String remoteIpAddr = P2.netsMap.get(hostId_rd).ipAddr;
                            int remotePortNum = P2.netsMap.get(hostId_rd).portNum;

                            // connect to the remote host
                            try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                            {

                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                                )
                                {
                                    // construct the send message with out
                                    Message sendMessage = new Message();
                                    sendMessage.command = "in";
                                    sendMessage.tuple = tuple;

                                    // send the message to remote server
                                    out.writeObject(sendMessage);
                                    System.out.println("Client: send \"in\" message with the tuple "
                                            + tuple.toString()
                                            + " to the host with IP " + remoteIpAddr);

                                    // construct the received object
                                    Message receivedMessage = null;

                                    try {
                                        if ((receivedMessage = (Message) in.readObject()) != null) {
                                            if (receivedMessage.success != true) {
                                                System.out.println("Error: remoted server failed to read the given tuple");
                                            } else if (receivedMessage.command.equals("in")) {
                                                System.out.println("Client: get tuple " + receivedMessage.tuple.toString() + " on " + remoteIpAddr);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            // broadcast all the host
                            System.out.println("Client: implicit type match query");
                            System.out.println("Client: broadcast all the hosts in the nets map");

                            // construct the broadcast message with command "in_broadcast"
                            Message sendMessage = new Message();
                            sendMessage.command = "in_broadcast";
                            sendMessage.tuple = parserEntry.tuple;

                            // thread pool
                            List<Thread> threadPool = new ArrayList<>();

                            // send out the broadcast messages
                            for (NetsEntry netsEntry : P2.netsMap.values()) {
                                String remoteIpAddr = netsEntry.ipAddr;
                                int remotePortNum = netsEntry.portNum;

                                Thread broadcastThread = new Thread(new ClientWorker("in_broadcast",
                                        parserEntry.tuple, remoteIpAddr, remotePortNum));
                                broadcastThread.start();
                                threadPool.add(broadcastThread);
                                System.out.println("Client: start broadcast thread with id: " + broadcastThread.getId());
                            }

                            // received message
                            Message receivedMessage = null;

                            // keep checking the broadcast message queue
                            while (receivedMessage == null) {
                                // NEED TO FIX
                                // to do ...
                                // if user A uses the client to do the implicit type match first, e.g. rd(?i:int)
                                // then another user B uses the client to put out(1) into the tuple space
                                // user A's client still gets stuck unless user A's console keeps printing stuff...
                                System.out.print("");
                                if (!Client.broadcastQueue.isEmpty()) {
                                    receivedMessage = Client.broadcastQueue.peek();
                                }
                            }

                            // terminate all the broadcast threads in the thread pool
                            for (Thread thread : threadPool) {
                                System.out.println("System: clean broadcast thread with id: " + thread.getId());
                                thread.interrupt();
                            }

                            // delete the received tuple
                            // connect to the remote host
                            try (Socket socket = new Socket(receivedMessage.ipAddr, receivedMessage.portNum);)
                            {
                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                                )
                                {
                                    // construct the send message with out
                                    Message sendSecondMessage = new Message();
                                    sendSecondMessage.command = "in";
                                    sendSecondMessage.tuple = receivedMessage.tuple;

                                    // send the message to remote server
                                    out.writeObject(sendSecondMessage);
                                    System.out.println("Client: send another \"in\" message with the tuple "
                                            + receivedMessage.tuple.toString()
                                            + " to the host with IP " + receivedMessage.ipAddr);

                                    // construct the received object
                                    Message receivedSecondMessage = null;

                                    try {
                                        if ((receivedSecondMessage = (Message) in.readObject()) != null) {
                                            if (receivedSecondMessage.success != true) {
                                                System.out.println("Error: remoted server failed to read the given tuple");
                                            } else if (receivedSecondMessage.command.equals("in")) {
                                                System.out.println("Client: get tuple " + receivedSecondMessage.tuple.toString() + " on " + receivedSecondMessage.ipAddr);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            System.out.println("get tuple " + receivedMessage.tuple.toString()
                                    + " on " + receivedMessage.ipAddr);

                            // turn off broadcast mode
                            Client.isBroadcast = false;
                            Client.broadcastQueue.clear();
                        }

                        state = "idle";
                        break;
                    }
                    case "exit": {
                        System.out.println("Exit linda!");
                        System.exit(0);
                        break;
                    }
                    default: {
                        // do something
                        state = "idle";
                        System.out.println("Error: invalid command");
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * client's broadcast worker
 */
class ClientWorker implements Runnable {
    private String command;
    private List<Object> tuple;
    private String ipAddr;
    private int portNum;

    public ClientWorker(String command, List<Object> tuple, String ipAddr, int portNum) {
        this.command = command;
        this.tuple = tuple;
        this.ipAddr = ipAddr;
        this.portNum = portNum;
    }

    @Override
    public void run() {
        // connect to the remote host
        try (Socket socket = new Socket(ipAddr, portNum);)
        {

            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            )
            {
                // construct message
                Message sendMessage = new Message();
                sendMessage.command = command;
                sendMessage.tuple = tuple;

                // send the message to remote server
                out.writeObject(sendMessage);
                System.out.println("Client: send broadcast message to the host with IP " + ipAddr);

                // construct the received object
                Message receivedMessage = null;

                try {
                    if ((receivedMessage = (Message) in.readObject()) != null) {
                        if (receivedMessage.success != true) {
                            System.out.println("System error: remote server failed to access the given tuple");
                        } else if (receivedMessage.command.equals("rd_broadcast") || receivedMessage.command.equals("in_broadcast")) {
                            Client.broadcastQueue.offer(receivedMessage);
                            System.out.println("Client: received tuple " + receivedMessage.tuple.toString()
                                    + " on " + receivedMessage.ipAddr);
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
