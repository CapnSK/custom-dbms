package Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;

public class LogService {
    private static Class clazz;
    private static LogService logger ;
    private static String ROOT_DIR = System.getProperty("user.dir");
    private static String DATA_DIR = "/data/";
    private static String LOG_FILE = "DBMS-logs.txt";
    private static FileWriter filewriter;

    static{
        File file = new File(ROOT_DIR+DATA_DIR+LOG_FILE);
        try {
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            if(!file.exists()){
                file.createNewFile();
            }
            if(file.exists() && file.isFile()){
                filewriter = new FileWriter(file, true);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static LogService getLogger(Class clazz){
        LogService.clazz = clazz;
        if(logger == null){
            logger = new LogService();
        }
        return logger;
    }

    public void log(String message){
        String prefix = clazz.getName()+"::"+LocalDate.now()+"::"+LocalTime.now()+"::";
        System.out.println(message);
        if(filewriter != null){
            try {
                BufferedWriter writer = new BufferedWriter(filewriter);
                writer.append(prefix+"::"+message+"\n");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}
