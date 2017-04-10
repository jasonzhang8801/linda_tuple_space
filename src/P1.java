import java.io.*;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...

    public static void main(String args[]) throws Exception {

        // assign host name
        // to do ...
        // check the user input
        if (args == null || args.length <= 0) {
            System.out.println("Error: invalid parameter");
        }

        String hostName = args[0];

        // create directories
        // to do ...
        // nets info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuples info: /tmp/<userlogin>/linda/<hostname>/tuples
        String infoDir = "/tmp/szhang/linda/" + hostName + "/";
        String netsFile = "nets.txt";
        String tuplesFile = "tuples.txt";
        new File(infoDir + netsFile).mkdirs();
        new File(infoDir + tuplesFile).mkdirs();

        // set up the server
        (new Thread(new Server(hostName))).start();
        System.out.println("The server named " + hostName + " started ...");


        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));)
        {
            String state = "idle";
            String[] tokens = null;

            while(true) {
                if (state == null) {
                    System.out.println("Error: state shouldn't be null");
                }

                switch(state.toLowerCase()) {
                    case "idle":
                        // read user input
                        System.out.print("linda> ");
                        String input = br.readLine();

                        // check the input
                        // to do ...
                        tokens = input.split("\\s");
                        state = tokens[0];
                        System.out.println("I am idling...");
                        break;
                    case "add":
                        // do something
                        String remoteIpAddr = tokens[1];
                        int remotePortNum = Integer.parseInt(tokens[2]);
//                        (new Thread(new Client(remoteIpAddr, remotePortNum))).start();

                        try (Socket socket = new Socket(remoteIpAddr, remotePortNum);)
                        {
                            // close out and in
                            // to do ...
                            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                 BufferedReader in = new BufferedReader(
                                         new InputStreamReader(socket.getInputStream()));)
                            {
                                String inputLine, outputLine;

                                outputLine = "Hello, I am Client!";
                                out.println(outputLine);

                                while ((inputLine = in.readLine()) != null) {
                                    System.out.println("Client: received message from the server: " + inputLine);
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        state = "idle";
                        break;
                    case "delete":
                        // do something
                        state = "idle";
                        break;
                    case "in":
                        // do something
                        state = "idle";
                        break;
                    case "out":
                        // do something
                        state = "idle";
                        break;
                    case "rd":
                        // do something
                        state = "idle";
                        break;
                    case "exit":
                        System.out.println("Exit linda!");
                        System.exit(0);
                        break;
                    default:
                        // do something
                        state = "idle";
                        System.out.println("Error: invalid command");
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
