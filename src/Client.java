import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class Client implements Runnable {
    private String remoteIpAddr;
    private int remotePortNum;

    public Client(String remoteIpAddr, int remotePortNum) {
        this.remoteIpAddr = remoteIpAddr;
        this.remotePortNum = remotePortNum;
    }

    @Override
    public void run() {

//        try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
//        {
//            // close out and in
//            // to do ...
//            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//                 BufferedReader in = new BufferedReader(
//                         new InputStreamReader(socket.getInputStream()));)
//            {
//                String inputLine, outputLine;
//
//                outputLine = "Hello, I am Client!";
//                out.println(outputLine);
//
//                while ((inputLine = in.readLine()) != null) {
//                    System.out.println("Client: received message from the server: " + inputLine);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

}
