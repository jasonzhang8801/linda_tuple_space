import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

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
            P2.netsMap.put(hostName, localHostInfo);

            // assign the ip and port back to P2 for print on the console
            P2.hostName = hostName;
            P2.ipAddr = ipAddr;
            P2.portNum = portNum;

            // listen on the port
            while (true) {
                (new Thread(new ServerWorker(serverSocket.accept()))).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
