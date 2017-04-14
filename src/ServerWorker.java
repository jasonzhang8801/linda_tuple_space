import java.io.*;
import java.net.Socket;

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
                            receivedMessage.netsMap = P1.netsMap;
                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send message " + receivedMessage.command);
                            break;
                        case "merge":
                            P1.netsMap = receivedMessage.netsMap;

                            // test only
                            // print out the nets map
//                            for (int id : P1.netsMap.keySet()) {
//                                System.out.println("Merged nets");
//                                System.out.println("key: " + id + " hostName: " + P1.netsMap.get(id).hostName  + " hostId: " + P1.netsMap.get(id).hostId);
//                            }

                            receivedMessage.success = true;
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send message " + receivedMessage.command);
                            break;
                        case "delete":
                            break;
                        case "in":
                            break;
                        case "out":

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
}
