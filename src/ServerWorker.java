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
                            out.writeObject(receivedMessage);
                            System.out.println("Server: send message " + receivedMessage.command);
                            break;
                        case "merge":

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


//        try (PrintWriter out =
//                     new PrintWriter(clientSocket.getOutputStream(), true);
//             BufferedReader in = new BufferedReader(
//                     new InputStreamReader(clientSocket.getInputStream()));)
//        {
//            String inputLine, outputLine;
//            outputLine = "Hello, I am Server";
//
//            while ((inputLine = in.readLine()) != null) {
//                System.out.println("Server: received message from the client: " + inputLine);
//                out.println(outputLine);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
