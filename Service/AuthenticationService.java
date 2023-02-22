package Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

import Model.User;

public class AuthenticationService {
    private static Scanner input = new Scanner(System.in);
    private static User activeUser;
    
    
    public static User getActiveUser() {
        return activeUser;
    }

    public static void setActiveUser(User activeUser) {
        AuthenticationService.activeUser = activeUser;
    }

    public static Boolean logInUser(){
        System.out.println("Enter username: ");
        String username = input.nextLine();
        System.out.println("Enter password: ");
        String password = input.nextLine();
        Object response = AuthenticationService.authenticate(username, password);
        if(response instanceof User){
            AuthenticationService.activeUser = (User) response;
            System.out.println("User logged In: "+activeUser.getUsername());
        } else if(response instanceof String){
            String errorMessage = (String)response;
            System.out.println(errorMessage);
        } 
        return response instanceof User ? true : false;
    }

    private static Object authenticate(String username, String password){
        Object response = null;
        if(username != null && password != null){
            if(!AuthenticationService.checkIfUserExists(username)){
                response = "User does not exist";
            }
            else{
                String encryptedPassword = null;
                try {
                    encryptedPassword = AuthenticationService.encryptPassword(password);
                } catch (NoSuchAlgorithmException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                User user = PersistenceService.getAllUsers().stream()
                                                .filter(Objects::nonNull)
                                                .filter(u->username.equals(u.getUsername())).findAny().orElse(null);
                if(user != null && user.getEncryptedPassword() != null && encryptedPassword != null){
                        if(user.getEncryptedPassword().equals(encryptedPassword)){
                            response = user;
                        } else {
                            response = "Invalid credentials";
                        }
                }
                else {
                    response = "User does not exist";
                }
            }
        }
        return response;
    }

    private static Boolean checkIfUserExists(String username){
        Boolean userExists = false;
        List<User> users = PersistenceService.getAllUsers();
        if(users != null && username != null){
            userExists = users.stream().anyMatch(user ->{
                return username.equals(user.getUsername());
            });
        }
        return userExists;
    }

    private static String encryptPassword(String password) throws NoSuchAlgorithmException{
        String encryptedPassword = password;
        MessageDigest MD5 = MessageDigest.getInstance("MD5");
        MD5.update(encryptedPassword.getBytes());
        encryptedPassword = new String(MD5.digest());
        return encryptedPassword;
    }
}
