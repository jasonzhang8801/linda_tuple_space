import java.io.*;
import java.lang.invoke.SwitchPoint;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 4/9/17.
 */
public class ServerWorker implements Runnable {

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
                            receivedMessage.netsMap = P1.netsMap;
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
                            }
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            // update the current host's nets map
                            P1.netsMap = receivedMessage.netsMap;

                            // test only
                            // print out the nets map
//                            for (int id : P1.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println("key: " + id + " hostName: " + P1.netsMap.get(id).hostName  + " hostId: " + P1.netsMap.get(id).hostId);
//                            }

                            // send back ACK message
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);

                            // update the netsMap into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P1.netsMapDir))) {
                                objOut.writeObject(P1.netsMap);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

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

                            // get the remote tuple
                            List<Object> receivedTuple = receivedMessage.tuple;

                            // get the hash of the tuple
                            String hashString = Utility.hashTuple(receivedTuple);

                            if (!P1.tupleSpace.containsKey(hashString)) {
                                // the first tuple with the hash string
                                // create a new tuple entry
                                TupleSpaceEntry tupleSpaceEntry = new TupleSpaceEntry();
                                tupleSpaceEntry.count = 1;
                                tupleSpaceEntry.tuple = receivedTuple;

                                // store the tuple space entry into the new linked list
                                List<TupleSpaceEntry> tupleLinkedList = new LinkedList<>();
                                tupleLinkedList.add(tupleSpaceEntry);

                                // put the tuple linked list into the tuple space
                                P1.tupleSpace.put(hashString, tupleLinkedList);

                                System.out.println("Server: new tuple " + receivedMessage.tuple.toString()
                                        + " in the tuple space with count " + tupleSpaceEntry.count);
                            } else {
                                // iterate to find the given tuple
                                List<TupleSpaceEntry> tupleLinkedList = P1.tupleSpace.get(hashString);
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
                                // the tuple space contains the hash string
                                // but the tuple is not in the tuple linked list
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);

                            // update the tupleSpace into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P1.tupleSpaceDir))) {
                                objOut.writeObject(P1.tupleSpace);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

//                            System.out.println("put tuple " + receivedMessage.tuple.toString() + " on " + P1.ipAddr);
                            break;
                        }
                        case "rd": {
                            // command rd
                            System.out.println("Server: received message with command \"rd\"");
                            List<Object> receivedTuple = receivedMessage.tuple;

                            // get the hash of the tuple
                            String hashString = Utility.hashTuple(receivedTuple);

                            List<Object> queriedTuple = null;
                            while (queriedTuple == null) {
                                queriedTuple = containsTuple(receivedTuple, command, P1.tupleSpace);
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the message with queried tuple " + queriedTuple.toString());

                            break;
                        }
                        case "in": {
                            // command in
                            System.out.println("Server: received message with command \"in\"");
                            List<Object> receivedTuple = receivedMessage.tuple;

                            List<Object> queriedTuple = null;
                            while (queriedTuple == null) {
                                queriedTuple = containsTuple(receivedTuple, command, P1.tupleSpace);
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.ipAddr = P1.ipAddr;
                            receivedMessage.portNum = P1.portNum;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back the message with queried tuple " + queriedTuple.toString());

                            // update the tupleSpace into the file
                            try (ObjectOutputStream objOut = new ObjectOutputStream(new FileOutputStream(P1.tupleSpaceDir))) {
                                objOut.writeObject(P1.tupleSpace);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            break;
                        }
                        case "rd_broadcast":
                        case "in_broadcast": {
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            List<Object> receivedTuple = receivedMessage.tuple;

                            List<Object> queriedTuple = null;
                            while (queriedTuple == null) {
                                queriedTuple = containsTuple(receivedTuple, command, P1.tupleSpace);
                            }

                            // send back ACK to inform
                            receivedMessage.success = true;
                            receivedMessage.tuple = queriedTuple;
                            receivedMessage.ipAddr = P1.ipAddr;
                            receivedMessage.portNum = P1.portNum;
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
            ConcurrentHashMap<String, List<TupleSpaceEntry>> tupleSpace)
    {
        // check the input
        if (tupleSpace == null || tupleSpace.size() == 0) return null;

        switch (command) {
            case "rd": {
                // get the hash string
                String hashString = Utility.hashTuple(receivedTuple);

                if (P1.tupleSpace.containsKey(hashString)) {
                    List<TupleSpaceEntry> tupleLinkedList = P1.tupleSpace.get(hashString);

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
                // get the hash string
                String hashString = Utility.hashTuple(receivedTuple);

                if (P1.tupleSpace.containsKey(hashString)) {
                    List<TupleSpaceEntry> tupleLinkedList = P1.tupleSpace.get(hashString);

                    // find if the received tuple in the linked list
                    for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                        List<Object> curTuple = tupleSpaceEntry.tuple;

                        // compare the tuples
                        if (comparesTuple(curTuple, receivedTuple)) {
                            // decrement the count for the tuple
                            tupleSpaceEntry.count--;
                            System.out.println("Server: the queried tuple with count " + tupleSpaceEntry.count);

                            // remove the tuple from the tuple space
                            if (tupleSpaceEntry.count == 0) {
                                P1.tupleSpace.remove(hashString);
                                System.out.println("Server: remove the queried tuple " + curTuple.toString());
                                System.out.println("Server: the tuple space contains the tuple "
                                        + curTuple.toString() + "? "
                                        + P1.tupleSpace.containsKey(hashString));
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
                for (List<TupleSpaceEntry> tupleLinkedList : P1.tupleSpace.values()) {
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
