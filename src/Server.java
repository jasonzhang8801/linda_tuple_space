import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class Server implements Runnable {
    // refactor
    // to do ...

    private String ipAddr;
    private int portNum;

    public Server(String ipAddr, int portNum) {
        this.ipAddr = ipAddr;
        this.portNum = portNum;
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(portNum, 0, InetAddress.getByName(null));
            Socket clientSocket = serverSocket.accept();

            // close out and in
            // to do ...
            PrintWriter out =
                    new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));

            String inputLine, outputLine;
            outputLine = "Hello, I am Server";

            while ((inputLine = in.readLine()) != null) {
                System.out.println("Server: received message from the client: " + inputLine);
                out.println(outputLine);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }


    public static void main(String args[]) {

    }
}
