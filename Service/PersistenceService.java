package Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

import Model.User;
import Utils.Constants;

public class PersistenceService {
    private static List<User> usersCache = null;

    private PersistenceService(){

    }
    private static String ROOT_DIR = System.getProperty("user.dir");
    private static String DATA_DIR = "/data/";
    private static String USER_STORE = "users.txt";


    public static Boolean storeUserCredentials(User user){
        Boolean userStored = true;
        if(user != null && user.getName() != null 
            && user.getEncryptedPassword() != null 
            && user.getSecurityQuestionAnswer() != null
        ){
            File file = new File(ROOT_DIR + DATA_DIR + USER_STORE);
            if(!file.exists()){
                try {
                    file.getParentFile().mkdirs();
                    userStored = file.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    userStored = false;
                }
            }
            if(file.isFile() && file.exists()){
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(file,true))) {
                    writer.append(user.getName());
                    writer.append(Constants.FILE_DELIMETER);
                    writer.append(user.getUsername());
                    writer.append(Constants.FILE_DELIMETER);
                    writer.append(user.getEncryptedPassword());
                    writer.append(Constants.FILE_DELIMETER);
                    writer.append(user.getSecurityQuestion());
                    writer.append(Constants.FILE_DELIMETER);
                    writer.append(user.getSecurityQuestionAnswer());
                    writer.append("\n");
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    userStored = false;
                }
            }
            else{
                userStored = false;
            }
        }
        if(userStored){
            PersistenceService.getAllUsers(true);
        }
        return userStored;
    }

    public static List<User> getAllUsers(){
        return getAllUsers(false);
    }

    public static List<User> getAllUsers(Boolean forceUpdate){
        if(forceUpdate || (usersCache == null || !usersCache.isEmpty())){

            if(usersCache == null){
                usersCache = new ArrayList<>();
            } else {
                usersCache.clear();
            }

            try (Scanner fileReader = new Scanner(new File(ROOT_DIR + DATA_DIR + USER_STORE))) {
                while(fileReader.hasNextLine()){
                    String input = fileReader.nextLine();
                    if(!input.equals("\n")){
                        List<String> data = Arrays.asList(input.split(Constants.FILE_DELIMETER.replace("$", "\\$"), -1));
                        if(data.size() == 5){
                            User user = new User(
                                data.get(0), 
                                data.get(1), 
                                null, 
                                data.get(3), 
                                data.get(4)
                            );
                            user.setEncryptedPassword(data.get(2));
                            usersCache.add(user);
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return usersCache;
    }

    public static Boolean dbExists(User user){
        Boolean dbFound = false;
        if(user != null && user.getUsername() != null){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername());
            if(file.exists() && file.isDirectory()){
                dbFound = Stream.of(file.listFiles())
                            .filter(File::isDirectory)
                            .filter(f->f.getName().equals(user.getUsername()))
                            .count() == 1;
            } else {
                dbFound = false;
            }
        }
        return dbFound;
    }

    public static Boolean createDB(User user, String dbName){
        Boolean dbCreated = false;;
        if(!dbExists(user)){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+dbName);
            file.mkdirs();
            dbCreated = file.exists();
        }
        return dbCreated;
    }


}
