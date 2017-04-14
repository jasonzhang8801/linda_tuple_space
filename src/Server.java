import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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

            // add command handler
            NetsEntry localHostInfo = new NetsEntry();
            localHostInfo.hostId = 0;
            localHostInfo.hostName = hostName;
            localHostInfo.ipAddr = ipAddr;
            localHostInfo.portNum = portNum;
            P1.netsMap.put(0, localHostInfo);

            // assign the ip and port back to P1 for print on the console
            P1.ipAddr = ipAddr;
            P1.portNum = portNum;

            // listen on the port
            while (true) {
                (new Thread(new ServerWorker(serverSocket.accept()))).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
