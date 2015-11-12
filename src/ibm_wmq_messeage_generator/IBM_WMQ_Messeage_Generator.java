package ibm_wmq_messeage_generator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Automatically get text files from server, put messages then log to database
 *
 * @author theanh@ilovex.co.jp
 */
public class IBM_WMQ_Messeage_Generator {

    /**
     * ****************************************************************************
     */
    // CONSTANT ZONE
    public static String CONFIG_FILE = "./main_config.ini";
    public static final int MAX_FILE_PROCESS_PER_TIME = 2;
    private static final int MAX_NUMBER_TRY_BEFORE_CLOSE_CONNECTION = 3;

    /**
     * ****************************************************************************
     */
    // GLOBAL VARIABLE ZONE
    private static Statement statement;
    private static Connection DBconnection;

    private static String SERVER_FOLDER_ACCESS_URL;
    private static String LOCAL_FOLDER_ACCESS_URL;
    private static String JDBC_CONNECT_STRING;
    private static String JDBC_DB_USER;
    private static String JDBC_DB_PASSWORD;
    private static int SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE;
    private static boolean isDBConnected = false;
    private static int try_number_not_found_new_file = 0;

    /**
     * main function
     *
     * @param args
     * @throws java.lang.InterruptedException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) {
            CONFIG_FILE = args[0];
        } else {
            System.out.println("Wrong command usage, Please use: java -jar \"main program jar URL\" \"main config URL\"");
            return;
        }

        try {
            loadConfiguration();

            createDBConnection();

            while (true) {
                int processed_file = putMessageFromFolder(SERVER_FOLDER_ACCESS_URL, LOCAL_FOLDER_ACCESS_URL);
                if (processed_file == 0) {
                    System.out.println("Not found new text file in server folder, Automatically recheck in " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE + " miliseconds.");
                    try_number_not_found_new_file++;
                    if (try_number_not_found_new_file == MAX_NUMBER_TRY_BEFORE_CLOSE_CONNECTION) {
                        closeDBConnection();
                    }
                    Thread.sleep(SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE);
                }
            }
//            closeDBConnection();
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can not read config file");
        }
    }

    /**
     * Make connection to database
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void createDBConnection() throws ClassNotFoundException, SQLException {
        if (isDBConnected) {
            return;
        }
        System.out.println("Connecting to DB");
//        Class.forName("oracle.jdbc.OracleDriver");
        DBconnection = DriverManager.getConnection(JDBC_CONNECT_STRING, JDBC_DB_USER, JDBC_DB_PASSWORD);
        System.out.println("DB connected");
        System.out.println("Auto commit: " + DBconnection.getAutoCommit());
        isDBConnected = true;
        statement = DBconnection.createStatement();
    }

    /**
     * Close connection to database
     *
     * @throws SQLException
     */
    public static void closeDBConnection() throws SQLException {
        if (!isDBConnected) {
            return;
        }
        isDBConnected = false;
        DBconnection.close();
        System.out.println("DB connection is closed");
    }

    /**
     * Get new text files, move them to local, put Messages to database and log
     * read file to database
     *
     * @param server_folder_address_url Server folder address that has text
     * @param local_folder_address_url Server folder address that has text files
     * @return The number of file has been processed
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static int putMessageFromFolder(String server_folder_address_url, String local_folder_address_url) throws SQLException, ClassNotFoundException {
        int process_file = 0;
        File server_folder = new File(server_folder_address_url);
        File local_folder = new File(local_folder_address_url);

        // Get file from server folder
        boolean one_time_flag = false;
        String last_read_file_name = "";
        int seq_no = 0;
        String last_folder_name = "";
        File[] server_file_list = server_folder.listFiles();
        if (server_file_list != null && server_file_list.length > 0) {
            Arrays.sort(server_file_list);
            for (File server_file : server_file_list) {
//                if (process_file + 1 > MAX_FILE_PROCESS_PER_TIME) {
//                    break;
//                }

                if (!server_file.isDirectory() && server_file.getName().endsWith(".txt")) {
                    if (!one_time_flag) {
                        // Try to create connection
                        createDBConnection();
                        try_number_not_found_new_file = 0;

                        // Get the last file that read
                        ResultSet rs = statement.executeQuery("SELECT FOLDERNAME, SEQNO, IFFILENAME FROM S_MSGFILE WHERE ROWNUM <= 1 ORDER BY FOLDERNAME DESC, SEQNO DESC");
                        while (rs.next()) {
                            last_folder_name = rs.getString("FOLDERNAME");
                            seq_no = rs.getInt("SEQNO");
                            last_read_file_name = rs.getString("IFFILENAME");
                            break;
                        }
                        if (last_read_file_name.isEmpty()) {
                            System.out.println(">>> Last read filename not found, Start reading all files.");
                        } else {
                            System.out.println(">>> Last folder name  : " + last_folder_name);
                            System.out.println(">>> Last SeqNo        : " + seq_no);
                            System.out.println(">>> Last read filename: " + last_read_file_name);
                        }
                        one_time_flag = true;
                    }

                    //check exist folder, if not create folder at local following: YYYYMMDD_HH
                    SimpleDateFormat format_child_local_folder_date = new SimpleDateFormat("YYYY_MM_dd_HH");
                    String child_local_folder_name = format_child_local_folder_date.format(new Date());
                    File child_local_folder = new File(local_folder.getAbsolutePath() + "/" + child_local_folder_name);
                    if (!child_local_folder.exists()) {
                        boolean mkdir_result = child_local_folder.mkdir();
                        if (mkdir_result) {
                            System.out.println("Created new folder: " + child_local_folder.getAbsolutePath());
                        } else {
                            System.out.println("Can not create new folder: " + child_local_folder.getAbsolutePath());
                        }
                    }
                    
                    // New child_local_folder_name -> should reset SeqNo
                    if (!last_folder_name.equalsIgnoreCase(child_local_folder_name))
                    {
                        last_folder_name = child_local_folder_name;
                        seq_no = 0;
                        System.out.println(">> New child_local_folder_name is set. Reset seq_no to 1");
                    }
                    
                    try {
                        //move file to local
                        Files.move(
                                server_file.toPath(),
                                FileSystems.getDefault().getPath(child_local_folder.getAbsolutePath() + "/" + server_file.getName()),
                                StandardCopyOption.REPLACE_EXISTING
                        );
                        System.out.println("Moved file from " + server_file.toPath().toString() + " to " + child_local_folder.getAbsolutePath() + "/" + server_file.getName());
                        File local_fle = new File(child_local_folder.getAbsolutePath() + "/" + server_file.getName());

                        //check read file or not
                        if (!last_read_file_name.isEmpty() && last_read_file_name.compareTo(local_fle.getName()) >= 0) {
                            continue;
                        }

                        // Read each line then insert Msg to DB
                        SimpleDateFormat format_insert_file_time = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss:SSS");
                        System.out.println("--- Reading file: " + local_fle.getName());
                        try {
                            FileReader in = new FileReader(local_fle);
                            BufferedReader br = new BufferedReader(in);
                            String line;
                            while ((line = br.readLine()) != null) {
                                if (!"".equalsIgnoreCase(line)) {
                                    seq_no++;
                                    putMesseageToDB(child_local_folder_name, seq_no, local_fle.getName(), line, format_insert_file_time.format(new Date()));
                                }
                            }
                        } catch (FileNotFoundException ex) {
                            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
                        }

                        process_file++;
                    } catch (IOException ex) {
                        Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
                        System.out.println("Can not move file from " + server_file.toPath().toString() + " to " + local_folder.toPath().toString());
                    }
                }
            }
        }
        return process_file;
    }

    private static void loadConfiguration() throws IOException {
        System.out.println("Start loading Configuration");
        Properties prop = new Properties();
        InputStream inputConfigStream = new FileInputStream(CONFIG_FILE);
        prop.load(inputConfigStream);
        SERVER_FOLDER_ACCESS_URL = prop.getProperty("server_folder_url");
        System.out.println("server_folder_url = " + SERVER_FOLDER_ACCESS_URL);
        LOCAL_FOLDER_ACCESS_URL = prop.getProperty("local_folder_url");
        System.out.println("local_folder_url = " + LOCAL_FOLDER_ACCESS_URL);
        JDBC_CONNECT_STRING = prop.getProperty("jdbc_connect_string");
        System.out.println("jdbc_connect_string = " + JDBC_CONNECT_STRING);
        JDBC_DB_USER = prop.getProperty("jdbc_user");
        System.out.println("jdbc_user = " + JDBC_DB_USER);
        JDBC_DB_PASSWORD = prop.getProperty("jdbc_password");
        System.out.println("jdbc_password = " + JDBC_DB_PASSWORD);
        try {
            SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE = Integer.parseInt(prop.getProperty("sleep_miliseconds_if_not_found_any_new_file"));
            System.out.println("sleep_miliseconds_if_not_found_any_new_file = " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE);
        } catch (NumberFormatException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Sleep time(miliseconds) must be integer");
        }
        System.out.println("Done loading Configuration");
    }

    private static void putMesseageToDB(String child_local_folder_name, int seq_no, String local_fle_name, String message_content, String insert_file_time_string) {
        // detact receivedMessage; receivedMessage must be in format: Inserted_time,SeqNo,content
        String[] parts = message_content.split("\",\""); // ","
        if (parts.length == 4) {
            String querry_string = "INSERT INTO S_MSGFILE(FOLDERNAME, SEQNO, IFFILENAME, KAIINKEY, ITEM01, ITEM02, ITEM03, INSERTDATE) VALUES ('"
                    + child_local_folder_name
                    + "'," + seq_no
                    + ",'" + local_fle_name
                    + "','" + parts[0].replaceAll("\"", "")
                    + "','" + parts[1].replaceAll("\"", "")
                    + "','" + parts[2].replaceAll("\"", "")
                    + "','" + parts[3].replaceAll("\"", "")
                    + "','" + insert_file_time_string + "')";
            try {
                statement.execute(querry_string);
            } catch (SQLException ex) {
                Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("Insert message to database failed: " + querry_string);
            }
        } else {
            System.out.println("Message Construction is not correct: " + message_content);
        }
    }
}
