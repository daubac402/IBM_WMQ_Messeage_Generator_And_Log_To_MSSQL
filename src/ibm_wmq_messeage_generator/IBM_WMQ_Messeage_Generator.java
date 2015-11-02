package ibm_wmq_messeage_generator;

import com.ibm.jms.JMSTextMessage;
import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueue;
import com.ibm.mq.jms.MQQueueConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.mq.jms.MQQueueSender;
import com.ibm.mq.jms.MQQueueSession;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * Automatically get text files from server, move them to local then put to MQ
 * and log to database
 *
 * @author theanh@ilovex.co.jp
 */
public class IBM_WMQ_Messeage_Generator {

    /**
     * ****************************************************************************
     */
    // CONSTANT ZONE
    public static final String HOST_NAME = "localhost";
    public static final int PORT_NUMBER = 1313;
    public static final String QUEUE_MANAGER_NAME = "QM_TEST";
    public static final String QUEUE_NAME = "Q2";
    public static final String LOGIN_USERNAME = "username";
    public static final String LOGIN_PASSWORD = "password";
    public static final int NUMBER_OF_MESSEAGE = 999999999; //max queue depth
//    public static final int     NUMBER_OF_MESSEAGE  = 10;
//    public static String SERVER_FOLDER_ACCESS_URL = "C:\\test_txt";
    public static String SERVER_FOLDER_ACCESS_URL = "S:\\temp\\LM_testdata";
    public static String LOCAL_FOLDER_ACCESS_URL = "C:\\test_txt";
    public static String JDBC_CONNECT_STRING = "jdbc:oracle:thin:user/pass@servername:port:orcl";
    public static String JDBC_DB_USER = "user";
    public static String JDBC_DB_PASSWORD = "pass";
    public static int SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE = 5000; // 5 seconds
    public static int MAX_FILE_PROCESS_PER_TIME = 2;
    /**
     * ****************************************************************************
     */
    // GLOBAL VARIABLE ZONE
    private static MQQueueConnection MQconnection;
    private static MQQueueSession session;
    private static MQQueueSender sender;
    private static Statement statement;
    private static Connection DBconnection;

    /**
     * main function
     *
     * @param args
     * @throws java.lang.InterruptedException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public static void main(String[] args) throws InterruptedException {
        try {
            createMQConnection();
            createDBConnection();

            while (true) {
                int processed_file = putMessageFromFolder(SERVER_FOLDER_ACCESS_URL, LOCAL_FOLDER_ACCESS_URL);
                if (processed_file == 0) {
                    System.out.println("Not found new text file in server folder, Automatically recheck in " + SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE + " miliseconds.");
                    Thread.sleep(SLEEP_MILISECOND_IF_NOT_FOUND_NEW_FILE);
                }
            }

//            closeMQConnection();
//            closeDBConnection();
        } catch (JMSException | ClassNotFoundException | SQLException ex) {
            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Make connection to IBM MQ
     *
     * @throws JMSException
     */
    public static void createMQConnection() throws JMSException {
        System.out.println("Connecting to MQ");
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
        cf.setHostName(HOST_NAME);
        cf.setPort(PORT_NUMBER);
        cf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        cf.setQueueManager(QUEUE_MANAGER_NAME);
        cf.setChannel("SYSTEM.DEF.SVRCONN");
        MQconnection = (MQQueueConnection) cf.createQueueConnection(LOGIN_USERNAME, LOGIN_PASSWORD);
        session = (MQQueueSession) MQconnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        MQQueue queue = (MQQueue) session.createQueue(QUEUE_NAME);
        sender = (MQQueueSender) session.createSender((Queue) queue);
        sender.setPriority(0); //if not, default is 4
        sender.setDeliveryMode(DeliveryMode.PERSISTENT); //DeliveryMode.PERSISTENT, the default
        MQconnection.start();
        System.out.println("MQ connected");
    }

    /**
     * Close connection to IBM MQ
     *
     * @throws JMSException
     */
    public static void closeMQConnection() throws JMSException {
        sender.close();
        session.close();
        MQconnection.close();
        System.out.println("DONE");
    }

    /**
     * Make connection to database
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void createDBConnection() throws ClassNotFoundException, SQLException {
        System.out.println("Connecting to DB");
//        Class.forName("oracle.jdbc.OracleDriver");
        DBconnection = DriverManager.getConnection(JDBC_CONNECT_STRING, JDBC_DB_USER, JDBC_DB_PASSWORD);
        System.out.println("DB connected");
        statement = DBconnection.createStatement();
    }

    /**
     * Close connection to database
     *
     * @throws SQLException
     */
    public static void closeDBConnection() throws SQLException {
        DBconnection.close();
    }

    /**
     * Make and put Message to IBM MQ
     *
     * @param content The Message's string content want to put to MQ
     * @throws JMSException
     */
    public static void putMesseageToMQ(String content) throws JMSException {
        JMSTextMessage message = (JMSTextMessage) session.createTextMessage(content);
        sender.send(message);
    }

    /**
     * generate random message by number_of_message and put to MQ
     *
     * @param number_of_message The number want to generate
     * @throws JMSException
     */
    public static void generateRandomMessageByNumberOfTimes(int number_of_message) throws JMSException {
        for (int i = 0; i < number_of_message; i++) {
            long uniqueNumber = System.currentTimeMillis() % 1000;
            putMesseageToMQ("random number: " + uniqueNumber);
        }
    }

    /**
     * Get new text files, move them to local, put to MQ and log to database
     *
     * @param server_folder_address_url Server folder address that has text
     * files
     * @param local_folder_address_url Server folder address that has text files
     * @return The number of file has been processed
     * @throws JMSException
     * @throws SQLException
     */
    private static int putMessageFromFolder(String server_folder_address_url, String local_folder_address_url) throws JMSException, SQLException {
        int process_file = 0;
        File server_folder = new File(server_folder_address_url);
        File local_folder = new File(local_folder_address_url);

        // Get file from server folder
        File[] server_file_list = server_folder.listFiles();
        if (server_file_list.length > 0) {
            Arrays.sort(server_file_list);
            
            // Get the last file that read
            ResultSet rs = statement.executeQuery("SELECT IFFILENAME FROM mqinsfile WHERE ROWNUM = 1 ORDER BY MQINSERTDATE DESC");
            String last_read_file_name = "";
            while (rs.next()) {
                last_read_file_name = rs.getString("IFFILENAME");
                break;
            }
            if (last_read_file_name.isEmpty()) {
                System.out.println(">>> Last read filename not found, Start reading all files.");
            } else {
                System.out.println(">>> Last read filename: " + last_read_file_name);
            }
            
            for (File server_file : server_file_list) {
//                if (process_file + 1 > MAX_FILE_PROCESS_PER_TIME) {
//                    break;
//                }

                if (!server_file.isDirectory() && server_file.getName().endsWith(".txt")) {
                    //check exist folder, if not create folder at local following: YYYYMMDD_HH
                    SimpleDateFormat format_child_local_folder_date = new SimpleDateFormat("YYYY_MM_dd_HH");
                    String child_local_folder_name = format_child_local_folder_date.format(new Date());
                    File child_local_folder = new File(local_folder.getAbsolutePath() + "\\" + child_local_folder_name);
                    if (!child_local_folder.exists()) {
                        boolean mkdir_result = child_local_folder.mkdir();
                        if (mkdir_result) {
                            System.out.println("Created new folder: " + child_local_folder.getAbsolutePath());
                        } else {
                            System.out.println("Can not create new folder: " + child_local_folder.getAbsolutePath());
                        }
                    }
                    try {
                        //move file to local
                        Files.move(
                                server_file.toPath(),
                                FileSystems.getDefault().getPath(child_local_folder.getAbsolutePath() + "\\" + server_file.getName()),
                                StandardCopyOption.REPLACE_EXISTING
                        );
                        System.out.println("Moved file from " + server_file.toPath().toString() + " to " + child_local_folder.getAbsolutePath() + "\\" + server_file.getName());
                        File local_fle = new File(child_local_folder.getAbsolutePath() + "\\" + server_file.getName());

                        //check read file or not
                        if (!last_read_file_name.isEmpty() && last_read_file_name.compareTo(local_fle.getName()) >= 0) {
                            continue;
                        }

                        // Read each line then insert Msg to MQ
                        System.out.println("--- Reading file: " + local_fle.getName());
                        try {
                            FileReader in = new FileReader(local_fle);
                            BufferedReader br = new BufferedReader(in);
                            String line;
                            while ((line = br.readLine()) != null) {
                                if (!"".equalsIgnoreCase(line)) {
                                    putMesseageToMQ(line);
                                }
                            }
                        } catch (FileNotFoundException ex) {
                            Logger.getLogger(IBM_WMQ_Messeage_Generator.class.getName()).log(Level.SEVERE, null, ex);
                        }

                        // Log read file to DB
                        System.out.println("+++ Marking As Read to Database: " + local_fle.getName());
                        SimpleDateFormat format_insert_file_time = new SimpleDateFormat("YYYY/MM/dd HH:mm:ss:SSS");
                        statement.execute("INSERT INTO mqinsfile VALUES ('" 
                                + format_insert_file_time.format(new Date())
                                + "','" + child_local_folder_name 
                                +"','" + local_fle.getName() +"')");

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
}
