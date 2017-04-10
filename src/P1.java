import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...

    public static void main(String args[]) throws Exception {
        //
        String ipAddr = "127.0.0.1";
        int serverPortNum;
        String hostName;

        // assign host name
        // to do ...
        // check the user input
        if (args == null || args.length <= 0) {
            System.out.println("Error: invalid parameter");
        }

        hostName = args[0];

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
        (new Thread(new Server())).start();
        System.out.println("The server named " + hostName + " started ...");

        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));)
        {
            while(true) {
                System.out.print("linda> ");
                String input = br.readLine();

                // check the input
                // to do ...
                String[] tokens = input.split("\\s");

                if (tokens[0].equals("add")) {
                    // set up the client
                    // to do ...
                    // a saperate function
                    String remoteIpAddr = tokens[1];
                    int remotePortNum = Integer.parseInt(tokens[2]);
                    (new Thread(new Client(remoteIpAddr, remotePortNum))).start();
                }

                if ("exit".equals(input)) {
                    System.out.println("Exit linda!");
                    System.exit(0);
                }

                System.out.println("User input: " + input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
