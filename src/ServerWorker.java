import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

    //  close out and in
    //  to do ...
        try (PrintWriter out =
                     new PrintWriter(clientSocket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(clientSocket.getInputStream()));)
        {
            String inputLine, outputLine;
            outputLine = "Hello, I am Server";

            while ((inputLine = in.readLine()) != null) {
                System.out.println("Server: received message from the client: " + inputLine);
                out.println(outputLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
