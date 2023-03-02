package Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

import Model.User;

/**
 * @apiNote This service takes care of handling authentication related processes of user
 */
public class AuthenticationService {
    private static Scanner input = new Scanner(System.in);
    private static User activeUser;
    private static LogService logger = LogService.getLogger(AuthenticationService.class);
    
    
    /** 
     * @apiNote To get current logged in user in the system
     * @return User
     */
    public static User getActiveUser() {
        return activeUser;
    }

    private static void setActiveUser(User activeUser) {
        AuthenticationService.activeUser = activeUser;
    }

    /**
     * @apiNote If the user needs to be logged in, use this API
     * This method will take care of taking user input about username and password
     * @return true if user logged in otherwise false
     */
    public static Boolean logInUser(){
        logger.log("Enter username: ");
        String username = input.nextLine();
        logger.log("Enter password: ");
        String password = input.nextLine();
        Object response = AuthenticationService.authenticate(username, password);
        if(response instanceof User){
            AuthenticationService.setActiveUser((User) response);
            logger.log("User logged In: "+activeUser.getUsername());
        } else if(response instanceof String){
            String errorMessage = (String)response;
            logger.log(errorMessage);
        } 
        return response instanceof User ? true : false;
    }

    /**
     * 
     * @param username
     * @param password
     * @return User model if log in succesfull otherwise appropriate error message
     */
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

    /**
     * To check if the user already exists
     * @param username
     * @return true if user exists else false
     */
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

    /**
     * creates a message digest of password to store in db
     * @param password
     * @return string of message digest
     * @throws NoSuchAlgorithmException
     */
    private static String encryptPassword(String password) throws NoSuchAlgorithmException{
        String encryptedPassword = password;
        MessageDigest MD5 = MessageDigest.getInstance("MD5");
        MD5.update(encryptedPassword.getBytes());
        encryptedPassword = new String(MD5.digest());
        return encryptedPassword;
    }
}
