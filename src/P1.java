import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by jasonzhang on 4/7/17.
 */
public class P1 {
    // refactor
    // to do ...

    public static void main(String args[]) throws Exception {
        // assign ip
        String ipAddr = "127.0.0.1";
        int serverPortNum;

        // create directories
        // to do ...
        // host info: /tmp/<userlogin>/linda/<hostname>/nets
        // tuple info: /tmp/<userlogin>/linda/<hostname>/tuples


        // assign host name
        // to do ...
        // store the host name in nets folder
        String hostName = "";

        // assign host name
        // to do ...
        // check the user input
        if (args == null || args.length <= 0) {
            System.out.println("Error: invalid parameter");
        }

        hostName = args[0];
        serverPortNum = Integer.parseInt(args[1]);

        // set up the server
        (new Thread(new Server(ipAddr, serverPortNum))).start();
        System.out.println("The server started ...");

        // print host ip and port
        System.out.println(ipAddr + " at port number: " + serverPortNum);

        // read user input
        BufferedReader br = null;

        try {
            br = new BufferedReader(new InputStreamReader(System.in));

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

                if ("Exit".equals(input)) {
                    System.out.println("Exit linda!");
                    System.exit(0);
                }

                System.out.println("User input: " + input);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
