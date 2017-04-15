import java.io.*;
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
                        case "add":
                            // send current host's nets map back
                            System.out.println("Server: received message with command " + receivedMessage.command);
                            receivedMessage.netsMap = P1.netsMap;
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send back message with the local nets map");
                            break;
                        case "merge":
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

                            System.out.println("Server: update the local nets map and send back ACK message");
                            break;
                        case "delete":
                            break;
                        case "in":
                            break;
                        case "out":
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
                                        + " in the tuple space");
                            } else {
                                // iterate to find the given tuple
                                List<TupleSpaceEntry> tupleLinkedList = P1.tupleSpace.get(hashString);
                                for (TupleSpaceEntry tupleSpaceEntry : tupleLinkedList) {
                                    List<Object> curTuple = tupleSpaceEntry.tuple;

                                    // check if there is a match
                                    if (compareTuple(receivedTuple, curTuple)) {
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
//                            System.out.println("put tuple " + receivedMessage.tuple.toString() + " on " + P1.ipAddr);
                            break;
                        case "rd":
                            break;
                        default:
                            System.out.println("Server: received unknown command");
                            break;
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean compareTuple(List<Object> t1, List<Object> t2) {
        // check the length of tuple
        if (t1.size() != t2.size()) {
            return false;
        } else {
            // check class type and value
            for (int i = 0; i < t1.size(); i++) {
                Object o1 = t1.get(i);
                Object o2 = t2.get(i);

                if (o1 instanceof String && o2 instanceof String) {
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
        return true;
    }
}
