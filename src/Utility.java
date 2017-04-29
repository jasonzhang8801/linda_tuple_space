import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 4/11/17.
 */
public abstract class Utility {

    /**
     * parse the stand input from the console
     * @param in
     * @return tokens
     */
    public static ParserEntry parser(String in) {
        ParserEntry parserEntry = new ParserEntry();

        // check input
        if (in == null || in.length() == 0) return null;

        // trim white space
        String trimmedIn = in.trim();

        // regex class
        Pattern pattern = null;
        Matcher matcher = null;

        pattern = Pattern.compile("^(add|delete|out|in|rd)");
        matcher = pattern.matcher(trimmedIn);

        if (!matcher.find()) {
            // no valid command name, e.g. add or in
            System.out.println("Error: no valid command, e.g. \"add\" or \"out\"");
            System.out.println("Please type command \"help\" to get more details");
            return null;
        } else {
            // check if the parenthesis is valid
            if (!isValidParenthesis(trimmedIn)) {
                System.out.println("Error: invalid parentheses");
                System.out.println("Please type command \"help\" to get more details");
                return null;
            }

            // command name, e.g. "add", "out"
            String commandName = matcher.group(1);
            parserEntry.commandName = commandName;

            String withoutCommandSubstr = trimmedIn.substring(matcher.end(1)).trim();

            switch (commandName.toLowerCase()) {
                case "add": {
                    // initialize the field: remote host information
                    parserEntry.remoteHostsInfo = new ArrayList<>();

                    // store the valid remote host info into remoteHostInfo
                    // e.g. "hostname, 123.456.78.90, 1234"
                    int start_pos = 0;
                    for (int i = 0; i < withoutCommandSubstr.length(); i++) {
                        char c = withoutCommandSubstr.charAt(i);

                        if (c == '(') {
                            start_pos = i;
                        } else if (c == ')') {
                            String content = withoutCommandSubstr.substring(start_pos + 1, i);

                            String remoteHostName = null;
                            String remoteIpAddr = null;
                            String remotePortNum = null;

                            // validate the remote host name
                            pattern = Pattern.compile("(^[a-zA-Z][a-zA-Z0-9]*)");
                            matcher = pattern.matcher(content);

                            if (matcher.find()) {
                                remoteHostName = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host name");
                                System.out.println("Help: please review the following host naming convention");
                                System.out.println("Host name must start with English letters");
                                System.out.println("Host name only contains English letters and numbers");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // validate the remote host ip
                            pattern = Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})");
                            matcher = pattern.matcher(content);

                            if (matcher.find()) {
                                remoteIpAddr = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host ip");
                                System.out.println("Help: please review the following IP address convention");
                                System.out.println("123.123.12.12");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // validate the remote host port
                            pattern = Pattern.compile(",\\s*(\\d{4,5}$)");
                            matcher = pattern.matcher(content);

                            if (matcher.find() && (Integer.parseInt(matcher.group(1)) >= 1024 && Integer.parseInt(matcher.group(1)) <= 49151)) {
                                remotePortNum = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host port");
                                System.out.println("Help: valid port number should be between 1024 and 49151, inclusive");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // NEED TO CHECK THE SEPARATOR
//                            // validate the separator, comma
//                            pattern = Pattern.compile(".+,\\s*.+,\\s*.+");
//                            matcher = pattern.matcher(content);
//                            if (!matcher.find())

                            // add ip and port number
                            parserEntry.remoteHostsInfo.add(remoteIpAddr);
                            parserEntry.remoteHostsInfo.add(remotePortNum);
                        }
                    }
                    break;
                }
                case "out": {
                    // initialize the field: tuple
                    parserEntry.tuple = new ArrayList<>();

                    // get the content between the parentheses
                    pattern = Pattern.compile("^\\((.*)\\)$");
                    matcher = pattern.matcher(withoutCommandSubstr);

                    if (!matcher.find()) {
                        System.out.println("Error: invalid tuple");
                        System.out.println("Please type command \"help\" to get more details");
                        return null;
                    } else {
                        String content = matcher.group(1).trim();

                        // assume no "," in the tuple
                        String[] splitted = content.split(",");

                        for (int i = 0; i < splitted.length; i++) {
                            // trim the tuple component
                            String s = splitted[i].trim();

                            if (isString(s)) {
                                parserEntry.tuple.add(s);
                            } else if (isInteger(s)) {
                                parserEntry.tuple.add(new Integer(s));
                            } else if (isFloat(s)) {
                                parserEntry.tuple.add(new Float(s));
                            } else {
                                System.out.println("Error: invalid tuple component type");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }
                        }

//                    // test only
//                    for (Object obj : parserEntry.tuple) {
//                        String className = obj.getClass().getName().split("\\.")[2];
//                        System.out.println("class name is " + className);
//                    }
                    }

                    break;
                }
                // both command "in" and "rd"
                case "in":
                case "rd": {
                    // initialize the field: tuple
                    parserEntry.tuple = new ArrayList<>();

                    // get the content between the parentheses
                    pattern = Pattern.compile("^\\((.*)\\)$");
                    matcher = pattern.matcher(withoutCommandSubstr);

                    if (!matcher.find()) {
                        System.out.println("Error: invalid tuple");
                        System.out.println("Please type command \"help\" to get more details");
                        return null;
                    } else {
                        String content = matcher.group(1).trim();

                        // assume no "," in the tuple
                        String[] splitted = content.split(",");

                        for (int i = 0; i < splitted.length; i++) {
                            // trim the tuple component
                            String s = splitted[i].trim();

                            if (isString(s)) {
                                parserEntry.tuple.add(s);
                            } else if (isInteger(s)) {
                                parserEntry.tuple.add(new Integer(s));
                            } else if (isFloat(s)) {
                                parserEntry.tuple.add(new Float(s));
                            } else {
                                // for variable type component
                                pattern = Pattern.compile("^\\?(.*):(.*)");
                                matcher = pattern.matcher(s);

                                if (!matcher.find()) {
                                    System.out.println("Error: invalid tuple component type");
                                    System.out.println("Please type command \"help\" to get more details");
                                    return null;
                                }

                                String varName = matcher.group(1).trim();
                                String typeName = matcher.group(2).trim();

                                // the string array to store the implicit type and variable
                                String[] implicitType = new String[2];
                                implicitType[0] = varName;

                                // convert the the type name to lower case and compare with defined type
                                if (typeName.toLowerCase().equals("int")) {
                                    implicitType[1] = "java.lang.Integer";
                                } else if (typeName.toLowerCase().equals("float")) {
                                    implicitType[1] = "java.lang.Float";
                                } else if (typeName.toLowerCase().equals("string")) {
                                    implicitType[1] = "java.lang.String";
                                } else {
                                    System.out.println("Error: invalid query on implicit type match");
                                    System.out.println("Help: please use the following format");
                                    System.out.println("?var:type where type could be \"int\" or \"float\" or \"string\"");
                                    return null;
                                }

                                parserEntry.tuple.add(implicitType);
                                Client.isBroadcast = true;
                            }
                        }

//                        // test only
//                        for (Object obj : parserEntry.tuple) {
//                            String className = obj.getClass().getName().split("\\.")[2];
//                            System.out.println("class name is " + className);
//                            System.out.println("component is " + obj.toString());
//                        }
                    }
                    break;
                }
                default: {
                    // no valid command name
                    break;
                }
            }
        }

        return parserEntry;
    }

    /**
     * check if the input is a valid integer
     * @param s
     * @return
     */
    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch(NumberFormatException e) {
            return false;
        } catch(NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    /**
     * check if the input is a valid string
     * @param s
     * @return
     */
    public static boolean isString(String s) {
        if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') return true;
        return false;
    }

    /**
     * check if the input is a valid float
     * @param s
     * @return
     */
    public static boolean isFloat(String s) {
        // the input string should has "."
        Pattern pattern = Pattern.compile("\\.");
        Matcher matcher = pattern.matcher(s);
        if (!matcher.find()) return false;

        try {
            Float.parseFloat(s);
        } catch(NumberFormatException e) {
            return false;
        } catch(NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    /**
     * helper function: validate if the parenthesis are valid
     * @param in
     * @return true if valid; false if invalid
     */
    private static boolean isValidParenthesis(String in) {
        // check input
        if (in == null || in.length() == 0) return false;

        // check if there is at least one parenthesis
        Pattern pattern = Pattern.compile("[\\(\\)]");
        Matcher matcher = pattern.matcher(in);
        if (!matcher.find()) return false;

        // create a stack to store the parenthesis
        Deque<Character> stack = new ArrayDeque<>();

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);

            // push the '(' into the stack
            if (c == '(') {
                stack.push(in.charAt(i));
            }
            // pop the ')' from the stack
            else if (c == ')') {
                if (stack.isEmpty()) {
                    return false;
                }
                stack.pop();
            }
        }

        if (!stack.isEmpty()) return false;
        return true;
    }

    /**
     * Hash the given tuple to hex string
     * @param tuple
     * @return host id
     */
    public static int hashToId(List<Object> tuple , int numOfHost) {
        // check the input
        if (tuple == null || tuple.size() == 0 || numOfHost <= 0) return -1;

        // host id
        int hostId = -1;

        // convert the tuple to string
        String hashString = hashTuple(tuple);

        int sum = 0;
        for (int i = 0; i < hashString.length(); i++) {
            String hex = Character.toString(hashString.charAt(i));
            sum += Integer.parseInt(hex, 16);
        }
//        System.out.println("sum: " + sum);
        hostId = sum % numOfHost;

        return hostId;
    }

    /**
     * hash the tuple to slot id
     * @param tuple
     * @return slot id
     */
    public static int hashToSlotId(List<Object> tuple) {
        // check the input
        if (tuple == null || tuple.size() == 0) return -1;

        // slot id
        int slotId = 0;

        // convert the tuple to hash string
        String hashString = hashTuple(tuple);

        for (int i = 0; i < hashString.length(); i++) {
            String hex = Character.toString(hashString.charAt(i));
            slotId += Integer.parseInt(hex, 16);
        }

        return slotId;
    }

    /**
     * convert tuple to hash string
     * MD5 -> 16 bytes or 128 bits
     * @param tuple
     * @return hash string
     */
    public static String hashTuple(List<Object> tuple) {
        // check the input
        if (tuple == null || tuple.size() == 0) return null;

        // convert the tuple to string
        // append each element to the string
        StringBuilder sb = new StringBuilder();
        for (Object obj : tuple) {
            String className = obj.getClass().getName().split("\\.")[2];
            sb.append(className + obj.toString());
        }
        String tupleStr = sb.toString();

        // hash string
        String hashString = null;

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(tupleStr.getBytes());
            byte[] digest = messageDigest.digest();
            BigInteger bigInt = new BigInteger(1, digest);
            hashString = bigInt.toString(16);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return hashString;
    }

    /**
     * convert the inserted index to netsMap's key hostName
     * @param index
     * @param netsMap
     * @return hostName
     */
    public static String netsMapIndexToKey(int index, LinkedHashMap<String, NetsEntry> netsMap) {
        if (netsMap == null || index < 0) return null;

        List<String> listOfHostName = new ArrayList<>(netsMap.keySet());

        return listOfHostName.get(index);
    }

    /**
     * convert the hostName to netsMap's inserted index
     * @param key
     * @param netsMap
     * @return index
     */
    public static int netsMapKeyToIndex(String key, LinkedHashMap<String, NetsEntry> netsMap) {
        if (netsMap == null || key == null) return -1;

        List<String> listOfHostName = new ArrayList<>(netsMap.keySet());

        return listOfHostName.indexOf(key);
    }

    /**
     * get the next host name in the netsMap
     * @param curHostName
     * @param netsMap
     * @return next host name
     */
    public static String getNextHostName(String curHostName, LinkedHashMap<String, NetsEntry> netsMap) {
        if (netsMap == null || curHostName == null) return null;

        int curHostIndex = netsMapKeyToIndex(curHostName, netsMap);
        if (curHostIndex < 0) {
            System.out.println("System error: wrong index for host name");
            return null;
        }

        int nextHostIndex = (curHostIndex + 1) % netsMap.size();

        return netsMapIndexToKey(nextHostIndex, netsMap);
    }

    /**
     * unit test
     * @param args
     */
    public static void main(String[] args) {
//        // hash function
//        List<Object> l = new ArrayList<>();
//        l.add(new Integer(10000000));
//        l.add(new String("def"));
//
//        int hostId = Utility.hashToSlotId(l);
//        System.out.println("host id: " + hostId);

//        // isInteger function
//        System.out.println("isInteger function");
//        System.out.println(Utility.isInteger("1")); // true
//        System.out.println(Utility.isInteger("1.1")); // false
//        System.out.println(Utility.isInteger("ab")); // false
//
//        // isFloat function
//        System.out.println("isFloat function");
//        System.out.println(Utility.isFloat("1.0f")); // true
//        System.out.println(Utility.isFloat("1..")); // false
//        System.out.println(Utility.isInteger("ab")); // false
//
//        // isString function
//        System.out.println("isString function");
//        System.out.println(Utility.isString("\"abc\"")); // true
//        System.out.println(Utility.isString("1.2")); // false
//        System.out.println(Utility.isString("abc")); // false

//        // isValidParenthesis function
//        System.out.println(isValidParenthesis("(a(b))")); // true
//        System.out.println(isValidParenthesis("(a(b)")); // false
//        System.out.println(isValidParenthesis("abc")); // false

//        // parser function add
//        String in = "add (host1, 123.456.78.90,   1025) (host2, 321.456.78.90,   5432)";
//        ParserEntry parserEntry = Utility.parser(in);
//        System.out.println("command is " + parserEntry.commandName);

//        // parser function out
//        String in = "out (\"abc\", 123,   1.5)";
//        ParserEntry parserEntry = Utility.parser(in);
//        System.out.println("command is " + parserEntry.commandName);

//        // parser function out
//        String in = "rd (\"abc\", 123,   ?i:integer)";
//        ParserEntry parserEntry = Utility.parser(in);
//        System.out.println("command is " + parserEntry.commandName);

        // get next host name
//        LinkedHashMap<String, NetsEntry> netsMap = new LinkedHashMap<>();
//        netsMap.put("h1", new NetsEntry());
//        netsMap.put("h2", new NetsEntry());
//        netsMap.put("h3", new NetsEntry());
//        netsMap.put("h4", new NetsEntry());
//
//        String curHostName = "h4";
//        String nextHostName = Utility.getNextHostName(curHostName, netsMap);
//        System.out.println("cur host name: " + curHostName + " next host name: " + nextHostName);

    }
}

class ParserEntry {
    // command name
    String commandName = null;

    // for add command
    // store remote host information
    List<String> remoteHostsInfo = null;

    // for non-add command
    // e.g. out, rd, in
    // store the tuple
    List<Object> tuple = null;
}

class NetsEntry implements Serializable {
    int hostId;
    String hostName;
    String ipAddr;
    int portNum;
}

class Message implements Serializable {
    // control field
    String command = null;
    boolean success = false;
    Const oriOrBu = null; // indicate if the tuple should put into the original tuple space or backUp tuple space

    // network information
    String ipAddr = null;
    int portNum;

    // data field
    LinkedHashMap netsMap = null; // store the host information
    List<Object> tuple = null;
    List<String> LUT = null; // look up table
    Map<String, List<Integer>> RLUT = null; // reversed look up table

}

enum Const {
    // tuple space type
    STRING,
    INTEGER,
    FLOAT,

    // user command
    // add related
    ADD,
    DEL,
    OUT,
    IN,
    RD,

    // system command
    REP_NETS, // for add, replace the old nets with new nets received
    REP_BU, // for del, replace the old backup tuple space with new backup received, redistribute old backup
    REP_LUTS, // for add, replace the old lookup tables with the new received, request the data

    // tuple space control
    ORI, // backUp tuple space
    BU // original tuple space
    ;

}



