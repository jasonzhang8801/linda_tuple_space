import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
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
                        case "merge": {
                            // validate the received message
                            if (receivedMessage.netsMap == null) {
                                System.out.println("System error: not received valid merged nets map");
                                return;
                            } else if (receivedMessage.LUT == null) {
                                System.out.println("System error: not received valid updated lookUp table");
                                return;
                            } else if (receivedMessage.RLUT == null) {
                                System.out.println("System error: not received valid updated reversed lookUp table");
                                return;
                            }

                            System.out.println("Server: received message with command " + receivedMessage.command);
                            // update the current host's netsMap, LUT and RLUT
                            P2.netsMap = receivedMessage.netsMap;
                            P2.LUT = receivedMessage.LUT;
                            P2.RLUT = receivedMessage.RLUT;

                            // test only
                            // print out the nets map
//                            int hostId = 0;
//                            for (String hostName : P2.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println(" hostName: " + hostName + " hostId: " + hostId++);
//                            }

                            // send back ACK message
                            receivedMessage.success = true;
                            receivedMessage.netsMap = null;
                            receivedMessage.LUT = null;
                            receivedMessage.RLUT = null;
                            out.writeObject(receivedMessage);

                            // update the netsMap into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P2.netsMapDir))) {
                                objOut.writeObject(P2.netsMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            System.out.println("Server: update the local nets map and send back ACK message");
                            break;
                        }
                        case "update_port": {
                            // validate the received message
                            if (receivedMessage.netsMap == null) {
                                System.out.println("System error: not received valid updated nets map");
                                return;
                            }

                            System.out.println("Server: received message with command " + receivedMessage.command);
                            // update the current host's netsMap
                            P2.netsMap = receivedMessage.netsMap;

                            // test only
                            // print out the nets map
//                            int hostId = 0;
//                            for (String hostName : P2.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println(" hostName: " + hostName + " hostId: " + hostId++);
//                            }

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
                        case "delete": {
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

                            if (!curTupleSpace.containsKey(slotId)) {
                                // the first tuple with the slot id
                                // create a new tuple entry
                                TupleSpaceEntry tupleSpaceEntry = new TupleSpaceEntry();
                                tupleSpaceEntry.count = 1;
                                tupleSpaceEntry.tuple = receivedTuple;

                                // store the tuple space entry into the new linked list
                                List<TupleSpaceEntry> tupleLinkedList = new LinkedList<>();
                                tupleLinkedList.add(tupleSpaceEntry);

                                // put the tuple linked list into the tuple space
                                curTupleSpace.put(slotId, tupleLinkedList);

                                System.out.println("Server: new tuple " + receivedMessage.tuple.toString()
                                        + " in the tuple space with count " + tupleSpaceEntry.count);
                            } else {
                                // iterate to find the given tuple
                                List<TupleSpaceEntry> tupleLinkedList = curTupleSpace.get(slotId);
                                for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                                    List<Object> curTuple = tupleSpaceEntry.tuple;

                                    // check if there is a match
                                    if (comparesTuple(curTuple, receivedTuple)) {
                                        // increment the conter in the tuple space entry
                                        tupleSpaceEntry.count += 1;

                                        System.out.println("Server: duplicated tuple " + receivedMessage.tuple.toString()
                                                +" with count " + tupleSpaceEntry.count);
                                    }
                                }

                                // HANDLE
                                // the tuple space contains the hash
                                // but the tuple is not in the tuple linked list
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

