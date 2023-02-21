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

import Model.User;

public class PersistenceService {
    private static List<User> usersCache = null;

    private PersistenceService(){

    }

    private static String delimiter = "_$_";
    private static String userStore = "/data/users.txt";


    public static Boolean storeUserCredentials(User user){
        Boolean userStored = true;
        if(user != null && user.getName() != null 
            && user.getEncryptedPassword() != null 
            && user.getSecurityQuestionAnswer() != null
        ){
            File file = new File(System.getProperty("user.dir") + userStore);
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
                    writer.append(delimiter);
                    writer.append(user.getUsername());
                    writer.append(delimiter);
                    writer.append(user.getEncryptedPassword());
                    writer.append(delimiter);
                    writer.append(user.getSecurityQuestion());
                    writer.append(delimiter);
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

            try (Scanner fileReader = new Scanner(new File(System.getProperty("user.dir") + userStore))) {
                while(fileReader.hasNextLine()){
                    String input = fileReader.nextLine();
                    if(!input.equals("\n")){
                        List<String> data = Arrays.asList(input.split(delimiter.replace("$", "\\$"), -1));
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
}
