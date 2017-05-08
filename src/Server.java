import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class Server implements Runnable {

    // refactor
    // to do ...
    private String ipAddr;
    private int portNum;
    private String hostName;

    public Server(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public void run() {

        try (ServerSocket serverSocket = new ServerSocket(0);)
        {
            // assign ip and port number
            ipAddr = InetAddress.getLocalHost().getHostAddress();
            portNum = serverSocket.getLocalPort();

            // assign the ip and port back to P2 for print on the console
            P2.hostName = hostName;
            P2.ipAddr = ipAddr;
            P2.portNum = portNum;

            // store the latest local host information
            NetsEntry localHostInfo = new NetsEntry();
            localHostInfo.hostName = hostName;
            localHostInfo.ipAddr = ipAddr;
            localHostInfo.portNum = portNum;

            // update the local host's information
            // when size == 0, boot up at the first time ever
            // when size == 1, directly kill the host when the host boots up at the first time
            // when size > 1, kill the host when the host is in the existing cluster of hosts
            P2.netsMap.put(hostName, localHostInfo);

            // listen on the port
            while (true) {
                (new Thread(new ServerWorker(serverSocket.accept()))).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ServerWorker implements Runnable {

    private Socket clientSocket;

    public ServerWorker(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {

        try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        )
        {
            // construct received message
            Message receivedMessage = null;

            try {
                if ((receivedMessage = (Message)in.readObject()) != null) {
                    // check which command the server received
                    String command = receivedMessage.command;

                    switch (command.toLowerCase()) {
                        case "add": {
                            // send current host's nets map back
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            receivedMessage.netsMap = P2.netsMap;
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back message with the local nets map");
                            break;
                        }
                        case "add_internal": {
                            // the local host is new one which will be added into the cluster
                            // need to redistribute the tuples

                            System.out.println("Server: received message with command " + receivedMessage.command);

                            // validate the recevied message
                            if (receivedMessage.LUT == null) {
                                System.out.println("System error: not received valid updated lookUp table");
                            } else if (receivedMessage.RLUT == null) {
                                System.out.println("System error: not received valid updated reversed lookUp table");
                                return;
                            }

                            P2.LUT = receivedMessage.LUT;
                            P2.RLUT = receivedMessage.RLUT;

                            // add local host into the reversed lookUp table
                            P2.RLUT.put(P2.hostName, new ArrayList<>());

                            // calculate the total number of host in updated netsMap
                            int numOfHost = P2.netsMap.size();

                            // redistribute tuple in original tuple space
                            // update LUT and RLUT
                            for (String remoteHostName : P2.netsMap.keySet()) {

                                // skip the local host
                                if (remoteHostName.equals(P2.hostName)) continue;

                                // remote host IP and port number
                                String remoteIpAddr = P2.netsMap.get(remoteHostName).ipAddr;
                                int remotePortNum = P2.netsMap.get(remoteHostName).portNum;

                                // truncate the given host's list of slot id
                                System.out.println("Remote host name is " + remoteHostName);

                                List<Integer> listOfTotalSlotId = P2.RLUT.get(remoteHostName);
                                int sizeOfTotalSlotId = listOfTotalSlotId.size();

                                System.out.println("total size of list of slot is " + sizeOfTotalSlotId);

                                List<Integer> listOfDelSlotId = new ArrayList<>(listOfTotalSlotId.subList(0, sizeOfTotalSlotId / numOfHost));

                                System.out.println("del size of list of slot is " + listOfDelSlotId.size());

                                List<Integer> listOfRestSlotId = new ArrayList<>(listOfTotalSlotId.subList(sizeOfTotalSlotId / numOfHost, sizeOfTotalSlotId));

                                System.out.println("rest size of list of slot is " + listOfRestSlotId.size());

                                // update the given host in RLUT
                                P2.RLUT.put(remoteHostName, listOfRestSlotId);

                                // update the local host in RLUT
                                P2.RLUT.get(P2.hostName).addAll(listOfDelSlotId);

                                // update the given host in LUT
                                for (int i = 0; i < listOfDelSlotId.size(); i++) {
                                    int slotId = listOfDelSlotId.get(i);
                                    P2.LUT.set(slotId, P2.hostName);
                                }

                                // WARNING
                                // server-server communication
                                try (Socket socket = new Socket(remoteIpAddr, remotePortNum);) {

                                    try(ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                        ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream());) {

                                        // construct the message with cmd del_slot
                                        Message sendMessageAdd = new Message();
                                        sendMessageAdd.command = "del_slot";
                                        sendMessageAdd.success = false;
                                        sendMessageAdd.listOfSlotId = listOfDelSlotId;
                                        sendMessageAdd.tupleSpace = null;
                                        sendMessageAdd.oriOrBu = Const.ORI;

                                        // send message
                                        objOut.writeObject(sendMessageAdd);

                                        // construct received message
                                        Message receivedMessageAdd = null;

                                        if ((receivedMessageAdd = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageAdd.tupleSpace != null && receivedMessageAdd.success) {
                                                P2.tupleSpace.putAll(receivedMessageAdd.tupleSpace);
                                                System.out.println("Server: successfully put tuples from the host with IP "
                                                        + remoteIpAddr +  " into tuple space");
                                            } else {
                                                System.out.println("Server: failed to redistribute tuples at host with IP "
                                                        + remoteIpAddr);
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            // backup the tuple space
                            // back up the tuple space
                            // determine which host is the backUp host
                            System.out.println("Server: back up ...");
                            String backUpHostName = Utility.getNextHostName(P2.hostName, P2.netsMap);
                            String backUpIpAddr = P2.netsMap.get(backUpHostName).ipAddr;
                            int backUpPortNum = P2.netsMap.get(backUpHostName).portNum;

                            try (Socket socket = new Socket(backUpIpAddr, backUpPortNum)) {

                                try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                    // construct send message
                                    Message sendMessageBackUp = new Message();
                                    sendMessageBackUp.command = "backup";
                                    sendMessageBackUp.oriOrBu = Const.BU;
                                    sendMessageBackUp.tupleSpace = P2.tupleSpace;
                                    sendMessageBackUp.success = false;

                                    objOut.writeObject(sendMessageBackUp);

                                    // construct received message
                                    Message receivedMessageBackUp = null;

                                    if ((receivedMessageBackUp = (Message) objIn.readObject()) != null) {
                                        if (receivedMessageBackUp.command.equals("backup") && receivedMessageBackUp.success) {
                                            System.out.println("Server: successfully back up tuple space at backup host with IP "
                                                    + backUpIpAddr);
                                        } else {
                                            System.out.println("Server: failed back up tuple space at host with IP "
                                                    + backUpIpAddr);
                                        }
                                    }
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            // update remote hosts LUT and RLUT
                            for (String remoteHostName : P2.netsMap.keySet()) {

                                // skip the local host
                                if (remoteHostName.equals(P2.hostName)) continue;

                                // remote host IP and port number
                                String remoteIpAddr = P2.netsMap.get(remoteHostName).ipAddr;
                                int remotePortNum = P2.netsMap.get(remoteHostName).portNum;

                                try (Socket socket = new Socket(remoteIpAddr, remotePortNum)) {

                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                         ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct the message with cmd replace_luts
                                        Message sendMessageRep = new Message();
                                        sendMessageRep.command = "replace_luts";
                                        sendMessageRep.LUT = P2.LUT;
                                        sendMessageRep.RLUT = P2.RLUT;
                                        sendMessageRep.success = false;

                                        // send the message
                                        objOut.writeObject(sendMessageRep);

                                        // construct the received message
                                        Message receivedMessageRep = null;

                                        if ((receivedMessageRep = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageRep.command.equals("replace_luts") && receivedMessageRep.success) {
                                                System.out.println("Server: successfully update lookUp table and reversed lookUp table "
                                                        + "at the host with IP " + remoteIpAddr);
                                            } else {
                                                System.out.println("Server: failed to update lookUp table and reversed lookUp table "
                                                        + "at the host with IP " + remoteIpAddr);
                                            }
                                        }

                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            }

                            // construct the ACK message
                            receivedMessage.success = true;
                            receivedMessage.LUT = null;
                            receivedMessage.RLUT = null;

                            out.writeObject(receivedMessage);
                            System.out.println("Server: successfully redistribute the tuple when adding new hosts");

                            break;
                        }
                        case "del_slot": {
                            // redistribute
                            // remove the given slot into tmpTupleSpace
                            // delete the given slot from the original tuple space
                            // send back the tmpTupleSpace

                            System.out.println("Server: received message with command " + receivedMessage.command);

                            //validate the received message
                            if (receivedMessage.listOfSlotId == null) {
                                System.out.println("System error: not received valid tuple space which should be deleted");
                                return;
                            } else if (receivedMessage.oriOrBu == null) {
                                System.out.println("System error: not indicate which tuple space which should be deleted");
                                return;
                            }

                            List<Integer> listOfSlotId = receivedMessage.listOfSlotId;

                            ConcurrentHashMap<Integer, List<TupleSpaceEntry>> tmpTupleSpace = new ConcurrentHashMap<>();

                            for (int i = 0; i < listOfSlotId.size(); i++) {
                                int slotId = listOfSlotId.get(i);

                                if (P2.tupleSpace.containsKey(slotId)) {
                                    tmpTupleSpace.put(slotId, P2.tupleSpace.get(slotId));

                                } else {
                                    System.out.println("System error: the tuple space doesn't contain the given slot");
                                    return;
                                }

                                P2.tupleSpace.remove(slotId);
                            }

                            // construct the message
                            receivedMessage.success = true;
                            receivedMessage.listOfSlotId = null;

                            // if the original tuple space, send back the tmpTupleSpace and ACK
                            // otherwise, just send ACK
                            if (receivedMessage.oriOrBu == Const.ORI) {
                                receivedMessage.tupleSpace = tmpTupleSpace;
                                out.writeObject(receivedMessage);
                                System.out.println("Server: send back the slots from original tuple space");

                                // back up the tuple space
                                // determine which host is the backUp host
                                System.out.println("Server: back up ...");
                                String backUpHostName = Utility.getNextHostName(P2.hostName, P2.netsMap);
                                String backUpIpAddr = P2.netsMap.get(backUpHostName).ipAddr;
                                int backUpPortNum = P2.netsMap.get(backUpHostName).portNum;

                                try (Socket socket = new Socket(backUpIpAddr, backUpPortNum)) {

                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                        ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct send message
                                        Message sendMessageBackUp = new Message();
                                        sendMessageBackUp.command = "backup";
                                        sendMessageBackUp.oriOrBu = Const.BU;
                                        sendMessageBackUp.tupleSpace = P2.tupleSpace;
                                        sendMessageBackUp.success = false;

                                        objOut.writeObject(sendMessageBackUp);

                                        // construct received message
                                        Message receivedMessageBackUp = null;

                                        if ((receivedMessageBackUp = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageBackUp.command.equals("backup") && receivedMessageBackUp.success) {
                                                System.out.println("Server: successfully back up tuple space at backup host with IP "
                                                + backUpIpAddr);
                                            } else {
                                                System.out.println("Server: failed back up tuple space at host with IP "
                                                        + backUpIpAddr);
                                            }
                                        }
                                    }

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            break;
                        }
                        case "put_slot": {
                            // redistribute
                            System.out.println("Server: received message with command " + receivedMessage.command);

                            // validate
                            if (receivedMessage.tupleSpace == null) {
                                System.out.println("Server: invalid tuple space");
                            } else if (receivedMessage.oriOrBu == null) {
                                System.out.println("Server: failed to indicate which tuple space");
                            }

                            if (receivedMessage.oriOrBu == Const.ORI) {
                                P2.tupleSpace.putAll(receivedMessage.tupleSpace);

                                // back up the tuple space
                                // determine which host is the backUp host
                                System.out.println("Server: back up ...");
                                String backUpHostName = Utility.getNextHostName(P2.hostName, P2.netsMap);
                                String backUpIpAddr = P2.netsMap.get(backUpHostName).ipAddr;
                                int backUpPortNum = P2.netsMap.get(backUpHostName).portNum;

                                try (Socket socket = new Socket(backUpIpAddr, backUpPortNum)) {

                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                         ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct send message
                                        Message sendMessageBackUp = new Message();
                                        sendMessageBackUp.command = "backup";
                                        sendMessageBackUp.oriOrBu = Const.BU;
                                        sendMessageBackUp.tupleSpace = P2.tupleSpace;
                                        sendMessageBackUp.success = false;

                                        objOut.writeObject(sendMessageBackUp);

                                        // construct received message
                                        Message receivedMessageBackUp = null;

                                        if ((receivedMessageBackUp = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageBackUp.command.equals("backup") && receivedMessageBackUp.success) {
                                                System.out.println("Server: successfully back up tuple space at backup host with IP "
                                                        + backUpIpAddr);
                                            } else {
                                                System.out.println("Server: failed back up tuple space at host with IP "
                                                        + backUpIpAddr);
                                            }
                                        }
                                    }

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            } else {
                                System.out.println("Server: shouldn't be backUp tuple space");
                            }

                            // construct the send message
                            receivedMessage.success = true;
                            receivedMessage.tupleSpace = null;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: successfully put the slots in the tuple space at host with IP "
                                    + P2.ipAddr);
                            break;
                        }
                        case "backup": {
                            // backup the tuple space at backUpTupleSpace
                            System.out.println("Server: received message with command " + receivedMessage.command);

                            if (receivedMessage.tupleSpace == null) {
                                System.out.println("System error: not receive valid tuple space for backup");
                            } else if (receivedMessage.oriOrBu == null) {
                                System.out.println("System error: not indicate which tuple space for backup");
                            }

                            P2.backUpTupleSpace = receivedMessage.tupleSpace;

                            // construct send message
                            receivedMessage.success = true;
                            receivedMessage.tupleSpace = null;

                            out.writeObject(receivedMessage);
                            System.out.println("Server: successfully back up the tuple space at local host with IP "
                                    + P2.ipAddr);

                            break;
                        }
                        case "replace_luts": {
                            // validate the received message
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            if (receivedMessage.LUT == null) {
                                System.out.println("System error: not received valid updated lookUp table");
                                return;
                            } else if (receivedMessage.RLUT == null) {
                                System.out.println("System error: not received valid updated reversed lookUp table");
                                return;
                            }

                            // update the current host's LUT and RLUT
                            P2.LUT = receivedMessage.LUT;
                            P2.RLUT = receivedMessage.RLUT;

                            // send back ACK message
                            receivedMessage.success = true;
                            receivedMessage.LUT = null;
                            receivedMessage.RLUT = null;
                            out.writeObject(receivedMessage);

                            System.out.println("Server: update the local lookUp table and reversed lookUp table");
                            break;
                        }
                        case "replace_luts_init": {
                            // validate the received message
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            if (receivedMessage.LUT == null) {
                                System.out.println("System error: not received valid updated lookUp table");
                                return;
                            } else if (receivedMessage.RLUT == null) {
                                System.out.println("System error: not received valid updated reversed lookUp table");
                                return;
                            }

                            // update the current host's LUT and RLUT
                            P2.LUT = receivedMessage.LUT;
                            P2.RLUT = receivedMessage.RLUT;

                            // initialize the tuple space according to reversed lookUp table
                            List<Integer> listOfSlotId = P2.RLUT.get(P2.hostName);
                            for (int i = 0; i < listOfSlotId.size(); i++) {
                                int slotId = listOfSlotId.get(i);
                                P2.tupleSpace.put(slotId, new ArrayList<>());
                            }
                            System.out.println("Server: initialize the tuple space");

                            // back up the tuple space
                            // determine which host is the backUp host
                            System.out.println("Server: back up ...");
                            String backUpHostName = Utility.getNextHostName(P2.hostName, P2.netsMap);
                            String backUpIpAddr = P2.netsMap.get(backUpHostName).ipAddr;
                            int backUpPortNum = P2.netsMap.get(backUpHostName).portNum;

                            try (Socket socket = new Socket(backUpIpAddr, backUpPortNum)) {

                                try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                    // construct send message
                                    Message sendMessageBackUp = new Message();
                                    sendMessageBackUp.command = "backup";
                                    sendMessageBackUp.oriOrBu = Const.BU;
                                    sendMessageBackUp.tupleSpace = P2.tupleSpace;
                                    sendMessageBackUp.success = false;

                                    objOut.writeObject(sendMessageBackUp);

                                    // construct received message
                                    Message receivedMessageBackUp = null;

                                    if ((receivedMessageBackUp = (Message) objIn.readObject()) != null) {
                                        if (receivedMessageBackUp.command.equals("backup") && receivedMessageBackUp.success) {
                                            System.out.println("Server: successfully back up tuple space at backup host with IP "
                                                    + backUpIpAddr);
                                        } else {
                                            System.out.println("Server: failed back up tuple space at host with IP "
                                                    + backUpIpAddr);
                                        }
                                    }
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            // send back ACK message
                            receivedMessage.success = true;
                            receivedMessage.LUT = null;
                            receivedMessage.RLUT = null;
                            out.writeObject(receivedMessage);

                            System.out.println("Server: update the local lookUp table and reversed lookUp table and "
                                    + "initialize the tuple space");

                            break;
                        }
                        case "replace_nets": {
                            System.out.println("Server: received message with command " + receivedMessage.command);

                            // validate the received message
                            if (receivedMessage.netsMap == null) {
                                System.out.println("System error: not received valid updated nets map");
                                return;
                            }

                            // update the current host's netsMap
                            P2.netsMap = receivedMessage.netsMap;

                            // update the netsMap into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.netsMapDir))) {
                                objOut.writeObject(P2.netsMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            // send back ACK message
                            receivedMessage.success = true;
                            receivedMessage.netsMap = null;
                            out.writeObject(receivedMessage);

                            System.out.println("Server: update the local nets map and send back ACK message");
                            break;
                        }
                        case "req_ts": {
                            // after failure, the host request for original and backUp tuple space for recovery
                            System.out.println("Server: received message with command " + receivedMessage.command);

                            // validate the received message
                            if (receivedMessage.oriOrBu == null) {
                                System.out.println("Server: failed to indicate which tuple space is requested");
                                return;
                            }

                            // construct the send message
                            if (receivedMessage.oriOrBu == Const.ORI) {
                                receivedMessage.tupleSpace = P2.tupleSpace;
                                System.out.println("Server: remote host request for original tuple space");
                            } else if (receivedMessage.oriOrBu == Const.BU) {
                                receivedMessage.tupleSpace = P2.backUpTupleSpace;
                                System.out.println("Server: remote host request for backUp tuple space");
                            } else {
                                System.out.println("Server: invalid tuple space request");
                            }

                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the tuple space to the request host");

                            break;
                        }
                        case "delete": {
                            System.out.println("Server: received message with command " + receivedMessage.command);

                            // validate the received message
                            if (receivedMessage.hostName == null) {
                                System.out.println("Server: failed to indicate which host should be deleted");
                                return;
                            } else if (receivedMessage.netsMap == null) {
                                System.out.println("Server: invalid netsMap");
                                return;
                            }

                            if (P2.hostName.equals(receivedMessage.hostName)) {
                                // construct the ACK message
                                receivedMessage.success = true;

                                out.writeObject(receivedMessage);
                                System.out.println("Server: will delete the local host");

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

                                System.exit(0);

                                break;
                            }

                            // update the local netsMap
                            P2.netsMap = receivedMessage.netsMap;
                            System.out.println("Server: update the netsMap without local host");

                            // redistribute the slots
                            // store the delete slots into temp list
                            List<Integer> tmpListOfSlot = new ArrayList<>(P2.RLUT.get(P2.hostName));
                            Map<String, List<Integer>> slotIdMap = new HashMap<>();

                            // split LUT and RLUT
                            // number of slot per host
                            int numOfSlot = tmpListOfSlot.size() / P2.netsMap.size();
                            System.out.println("numOfSlot is " + numOfSlot);

                            // allocate the slots
                            int start = 0;
                            int end = numOfSlot;

                            List<String> listOfHostName = new ArrayList<>(P2.netsMap.keySet());
                            for (int i = 0; i < listOfHostName.size(); i++) {
                                System.out.println("start " + start + " end " + end);

                                String hostName = listOfHostName.get(i);

                                List<Integer> subListOfSlot = new ArrayList<>(tmpListOfSlot.subList(start, end));

                                slotIdMap.put(hostName, subListOfSlot);

                                // update RLUT
                                P2.RLUT.get(hostName).addAll(subListOfSlot);

                                // update the LUT
                                for (int j = 0; j < subListOfSlot.size(); j++) {
                                    P2.LUT.set(subListOfSlot.get(j), hostName);
                                }

                                // update the start index and end index
                                start = end;
                                // check if the host is the second from the end
                                if (i == listOfHostName.size() - 2) {
                                    end = tmpListOfSlot.size();
                                } else {
                                    end += numOfSlot;
                                }
                            }
                            System.out.println("Client: successfully reallocate the slot to look up table and reversed look up table");

                            // remove the list of slot from RLUT
                            P2.RLUT.remove(P2.hostName);
                            System.out.println("Server: remove the list of slot from reversed lookUp table");

                            for (String hostName : slotIdMap.keySet()) {
                                System.out.println("hostName " + hostName + " listOfSlot " + slotIdMap.get(hostName).size());
                            }


                            for (String hostName : P2.netsMap.keySet()) {
                                // temp tuple space
                                ConcurrentHashMap<Integer, List<TupleSpaceEntry>> tmpTupleSpace = new ConcurrentHashMap<>();

                                List<Integer> listOfSlot = slotIdMap.get(hostName);

                                for (int i = 0; i < listOfSlot.size(); i++) {
                                    int slotId = listOfSlot.get(i);
                                    List<TupleSpaceEntry> tmpListOfTupleSpaceEntry = new ArrayList<>(P2.tupleSpace.get(slotId));
                                    P2.tupleSpace.remove(slotId);

                                    tmpTupleSpace.put(slotId, tmpListOfTupleSpaceEntry);
                                }

                                String remoteAddr = P2.netsMap.get(hostName).ipAddr;
                                int remotePortNum = P2.netsMap.get(hostName).portNum;

                                try (Socket socket = new Socket(remoteAddr, remotePortNum)) {

                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                         ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct send message
                                        Message sendMessageDel = new Message();
                                        sendMessageDel.command = "put_slot";
                                        sendMessageDel.tupleSpace = tmpTupleSpace;
                                        sendMessageDel.oriOrBu = Const.ORI;
                                        sendMessageDel.success = false;

                                        objOut.writeObject(sendMessageDel);

                                        // construct received message
                                        Message receivedMessageDel = null;

                                        if ((receivedMessageDel = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageDel.command.equals("put_slot") && receivedMessageDel.success) {
                                                System.out.println("Server: successfully redistribute the slots to remote host with IP "
                                                        + remoteAddr);
                                            } else {
                                                System.out.println("Server: fail to redistribute the slots to remote host with IP "
                                                        + remoteAddr);
                                            }
                                        }
                                    }

                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }


                            // update remote hosts LUT and RLUT
                            for (String remoteHostName : P2.netsMap.keySet()) {

                                // skip the local host
                                if (remoteHostName.equals(P2.hostName)) continue;

                                // remote host IP and port number
                                String remoteIpAddr = P2.netsMap.get(remoteHostName).ipAddr;
                                int remotePortNum = P2.netsMap.get(remoteHostName).portNum;

                                try (Socket socket = new Socket(remoteIpAddr, remotePortNum)) {

                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                         ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct the message with cmd replace_luts
                                        Message sendMessageRep = new Message();
                                        sendMessageRep.command = "replace_luts";
                                        sendMessageRep.LUT = P2.LUT;
                                        sendMessageRep.RLUT = P2.RLUT;
                                        sendMessageRep.success = false;

                                        // send the message
                                        objOut.writeObject(sendMessageRep);

                                        // construct the received message
                                        Message receivedMessageRep = null;

                                        if ((receivedMessageRep = (Message) objIn.readObject()) != null) {
                                            if (receivedMessageRep.command.equals("replace_luts") && receivedMessageRep.success) {
                                                System.out.println("Server: successfully update lookUp table and reversed lookUp table "
                                                        + "at the host with IP " + remoteIpAddr);
                                            } else {
                                                System.out.println("Server: failed to update lookUp table and reversed lookUp table "
                                                        + "at the host with IP " + remoteIpAddr);
                                            }
                                        }

                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            }

                            // construct the ACK message
                            receivedMessage.success = true;

                            out.writeObject(receivedMessage);
                            System.out.println("Server: successfully redistribute the slots to other hosts when deleting");

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

                            System.exit(0);

                            break;
                        }
                        case "out": {
                            // validate the received message
                            if (receivedMessage.tuple == null) {
                                System.out.println("System error: not received valid tuple");
                                return;
                            }

                            // check which tuple space should use, original or backUp
                            ConcurrentHashMap<Integer, List<TupleSpaceEntry>> curTupleSpace = null;
                            if (receivedMessage.oriOrBu == Const.ORI) {
                                curTupleSpace = P2.tupleSpace;
                                System.out.println("Server: the tuple should be stored in the original tuple space");
                            } else if (receivedMessage.oriOrBu == Const.BU) {
                                curTupleSpace = P2.backUpTupleSpace;
                                System.out.println("Server: the tuple should be stored in the backUp tuple space");
                            } else {
                                System.out.println("System error: failed to indicate which tuple space");
                                break;
                            }

                            // get the remote tuple
                            List<Object> receivedTuple = receivedMessage.tuple;

                            // get the slot id by hashing the tuple
                            int slotId = Utility.hashToSlotId(receivedTuple);

                            // after initialization, each host's tuple space should contain the slot
                            // the slot may be empty
                            if (curTupleSpace.containsKey(slotId)) {

                                // check if the tuple in the slot
                                boolean isInSlot = false;

                                // iterate to find the given tuple
                                List<TupleSpaceEntry> tupleLinkedList = curTupleSpace.get(slotId);
                                for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                                    List<Object> curTuple = tupleSpaceEntry.tuple;

                                    // check if there is a match
                                    if (comparesTuple(curTuple, receivedTuple)) {
                                        // mark as the tuple is in the slot
                                        isInSlot = true;

                                        // increment the conter in the tuple space entry
                                        tupleSpaceEntry.count += 1;

                                        System.out.println("Server: duplicated tuple " + receivedMessage.tuple.toString()
                                                +" with count " + tupleSpaceEntry.count);
                                    }
                                }

                                // the given tuple never in the slot before
                                if (!isInSlot) {
                                    // create a new tuple entry
                                    TupleSpaceEntry tupleSpaceEntry = new TupleSpaceEntry();
                                    tupleSpaceEntry.count = 1;
                                    tupleSpaceEntry.tuple = receivedTuple;

                                    tupleLinkedList.add(tupleSpaceEntry);
                                    System.out.println("Server: new tuple " + receivedMessage.tuple.toString()
                                        + " in the tuple space with count " + tupleSpaceEntry.count);

                                }
                            } else {
                                System.out.println("System error: the given slot is not in the tuple space");
                            }

//                                // HANDLE
//                                // the tuple space contains the hash
//                                // but the tuple is not in the tuple linked list
//                            }

                            // update the tupleSpace into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.tupleSpaceDir))) {
                                TupleSpace tupleSpace = new TupleSpace();
                                tupleSpace.tupleSpace = P2.tupleSpace;
                                tupleSpace.backUpTupleSpace = P2.backUpTupleSpace;
                                objOut.writeObject(tupleSpace);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.tuple = null;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send ACK message back for command \"out\"");

                            break;
                        }
                        case "rd": {
                            // command rd
                            System.out.println("Server: received message with command \"rd\"");
                            List<Object> receivedTuple = receivedMessage.tuple;

                            // check which tuple space should use, original or backUp
                            ConcurrentHashMap<Integer, List<TupleSpaceEntry>> curTupleSpace = null;
                            if (receivedMessage.oriOrBu == Const.ORI) {
                                curTupleSpace = P2.tupleSpace;
                                System.out.println("Server: the tuple should be read in the original tuple space");
                            } else if (receivedMessage.oriOrBu == Const.BU) {
                                curTupleSpace = P2.backUpTupleSpace;
                                System.out.println("Server: the tuple should be read in the backUp tuple space");
                            } else {
                                System.out.println("System error: failed to indicate which tuple space");
                                break;
                            }

                            List<Object> queriedTuple = null;
                            while (queriedTuple == null) {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                queriedTuple = containsTuple(receivedTuple, command, curTupleSpace);
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.tuple = null;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the message with queried tuple " + queriedTuple.toString());

                            break;
                        }
                        case "in": {
                            // command in
                            System.out.println("Server: received message with command \"in\"");
                            List<Object> receivedTuple = receivedMessage.tuple;

                            // check which tuple space should use, original or backUp
                            ConcurrentHashMap<Integer, List<TupleSpaceEntry>> curTupleSpace = null;
                            if (receivedMessage.oriOrBu == Const.ORI) {
                                curTupleSpace = P2.tupleSpace;
                                System.out.println("Server: the tuple should be removed from the original tuple space");
                            } else if (receivedMessage.oriOrBu == Const.BU) {
                                curTupleSpace = P2.backUpTupleSpace;
                                System.out.println("Server: the tuple should be removed from the backUp tuple space");
                            } else {
                                System.out.println("System error: failed to indicate which tuple space");
                                break;
                            }

                            List<Object> queriedTuple = null;

                            while (queriedTuple == null) {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                queriedTuple = containsTuple(receivedTuple, command, curTupleSpace);
                            }

                            // update the tupleSpace into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.tupleSpaceDir))) {
                                TupleSpace tupleSpace = new TupleSpace();
                                tupleSpace.tupleSpace = P2.tupleSpace;
                                tupleSpace.backUpTupleSpace = P2.backUpTupleSpace;
                                objOut.writeObject(tupleSpace);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.tuple = null;
                            receivedMessage.ipAddr = P2.ipAddr;
                            receivedMessage.portNum = P2.portNum;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the message with queried tuple " + queriedTuple.toString());

                            break;
                        }
                        case "rd_broadcast":
                        case "in_broadcast": {
                            // TO DO ...
                            // ori or backUp

                            System.out.println("Server: received message with command " + receivedMessage.command);
                            List<Object> receivedTuple = receivedMessage.tuple;

                            List<Object> queriedTuple = null;
                            while (queriedTuple == null) {
                                queriedTuple = containsTuple(receivedTuple, command, P2.tupleSpace);
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.tuple = queriedTuple;
                            receivedMessage.ipAddr = P2.ipAddr;
                            receivedMessage.portNum = P2.portNum;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the message with queried tuple " + queriedTuple.toString());

                            break;
                        }
                        default: {
                            System.out.println("Server: received unknown command");
                            break;
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // NEED TO REFACTOR
    // to do ..
    // there should be a dedicated class Tuple
    // class Tuple has the method comparesTuple()
    /**
     * compare if the two tuple is the same
     * @param t1
     * @param t2
     * @return true if same; false if different
     */
    private boolean comparesTuple(List<Object> t1, List<Object> t2) {

        // check the length of tuple
        if (t1.size() != t2.size()) {
            return false;
        } else {
            // check class type and value
            for (int i = 0; i < t1.size(); i++) {
                Object o1 = t1.get(i);
                Object o2 = t2.get(i);

                // implicit type match
                if (o1.getClass().getName().equals("[Ljava.lang.String;")
                        || o2.getClass().getName().equals("[Ljava.lang.String;")) {

                    // both are implicit type match
                    if (o1.getClass().getName().equals("[Ljava.lang.String;")
                            && o2.getClass().getName().equals("[Ljava.lang.String;")) {
                        String[] implicitType1 = (String[]) o1;
                        String[] implicitType2 = (String[]) o2;

                        if (!implicitType1[1].equals(implicitType2[1])) {
                            return false;
                        }
                    }
                    // one of them is implicit type match
                    else if (o1.getClass().getName().equals("[Ljava.lang.String;")) {

                        String[] implicitType = (String[]) o1;
                        if (!implicitType[1].equals(o2.getClass().getName())) {
                            return false;
                        }

                    } else {

                        String[] implicitType = (String[]) o2;
                        if (!implicitType[1].equals(o1.getClass().getName())) {
                            return false;
                        }
                    }
                } else {
                    // exact type match
                    // not the same type
                    if (!o1.getClass().getName().equals(o2.getClass().getName())) {
                        return false;
                    } else if (o1 instanceof String && o2 instanceof String) {
                        String s1 = (String) o1;
                        String s2 = (String) o2;
//                    System.out.println("s1: " + s1 + " s2: " + s2);
                        if (!s1.equals(s2)) {
                            return false;
                        }
                    } else if (o1 instanceof Integer && o2 instanceof Integer) {
                        int i1 = (Integer) o1;
                        int i2 = (Integer) o2;
//                    System.out.println("i1: " + i1 + " i2: " + i2);
                        if (i1 != i2) {
                            return false;
                        }
                    } else if (o1 instanceof Float && o2 instanceof Float) {
                        float f1 = (Float) o1;
                        float f2 = (Float) o2;
//                    System.out.println("f1: " + f1 + " f2: " + f2);
                        if (f1 != f2) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * check if the received tuple exits in the tuple space
     * @param receivedTuple
     * @param command
     * @param tupleSpace
     * @return queried tuple, otherwise null
     */
    private List<Object> containsTuple(
            List<Object> receivedTuple,
            String command,
            ConcurrentHashMap<Integer, List<TupleSpaceEntry>> tupleSpace)
    {
        // check the input
        if (receivedTuple == null || command == null || tupleSpace == null || tupleSpace.size() == 0) return null;

        switch (command) {
            case "rd": {
                // get the slot id
                int slotId = Utility.hashToSlotId(receivedTuple);

                if (tupleSpace.containsKey(slotId)) {
                    List<TupleSpaceEntry> tupleLinkedList = tupleSpace.get(slotId);

                    // find if the received tuple in the linked list
                    for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                        List<Object> curTuple = tupleSpaceEntry.tuple;

                        // compare the tuples
                        if (comparesTuple(curTuple, receivedTuple)) {
                            return curTuple;
                        }
                    }
                }

                return null;
            }
            case "in": {
                // get the slot id
                int slotId = Utility.hashToSlotId(receivedTuple);

                if (tupleSpace.containsKey(slotId)) {
                    List<TupleSpaceEntry> tupleLinkedList = tupleSpace.get(slotId);

                    // find if the received tuple in the linked list
                    for (int i = 0; i < tupleLinkedList.size(); i++) {
                        TupleSpaceEntry tupleSpaceEntry = tupleLinkedList.get(i);
                        List<Object> curTuple = tupleSpaceEntry.tuple;

                        // compare the tuples
                        if (comparesTuple(curTuple, receivedTuple)) {
                            // decrement the count for the tuple
                            tupleSpaceEntry.count--;
                            System.out.println("Server: the queried tuple with count " + tupleSpaceEntry.count);

                            // remove the tuple from the tuple space
                            if (tupleSpaceEntry.count == 0) {
                                tupleLinkedList.remove(i);
                                System.out.println("Server: remove the queried tuple " + curTuple.toString());
                                System.out.println("Server: the queried tuple with count " + tupleSpaceEntry.count);
                            }
                            return curTuple;
                        }
                    }
                }
                return null;
            }
            case "rd_broadcast":
            case "in_broadcast": {
                // go through the whole tuple space
                for (List<TupleSpaceEntry> tupleLinkedList : tupleSpace.values()) {
                    for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                        List<Object> curTuple = tupleSpaceEntry.tuple;

                        if (comparesTuple(curTuple, receivedTuple)) {
                            System.out.println("Server: match the queried tuple " + curTuple.toString()
                                    +" with count " + tupleSpaceEntry.count);
                            return curTuple;
                        }
                    }
                }

                break;
            }
            default: {
                System.out.println("System error: no such command " + command);
                return null;
            }
        }

        return null;
    }
}

