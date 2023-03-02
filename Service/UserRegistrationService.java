package Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import Model.User;

public class UserRegistrationService {
    private static LogService logger = LogService.getLogger(UserRegistrationService.class);

    private static String encryptionAlgorithm = "MD5";

    private static Scanner input = new Scanner(System.in);
    
    /** 
     * returns true if the user is registered successfully
     * @return Boolean
     */
    public static Boolean registerUser(){
        User user = UserRegistrationService.createUserModel();
        try {
            user.setEncryptedPassword(UserRegistrationService.encryptPassword(user.getPassword()));
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Boolean userStored = PersistenceService.storeUserCredentials(user);
        if(!userStored){
            logger.log("Could not register user. Please try again");
        } else {
            logger.log("User registered successfully, please login to continue");
        }
        return userStored;
    }

    /**
     * creates user model by taking input from console
     * @return
     */
    private static User createUserModel(){
        logger.log("Please enter your name: ");
        String name = input.nextLine();
        logger.log("Please enter preferred username (should be unique): ");
        String userName = null;
        Boolean userNameTaken = true;
        do{
            userName = input.nextLine();
            userNameTaken = UserRegistrationService.checkIfUserExists(userName);
            if(userNameTaken){
                logger.log("This user name is taken, please enter a different username: ");
            }
        } while(userNameTaken);

        logger.log("Please enter your password: ");
        String password = input.nextLine();
        logger.log("Re enter your password: ");
        String passwordCheck = input.nextLine();

        while(!password.equals(passwordCheck)){
            logger.log("Both password do not match, please try again");
            passwordCheck = input.nextLine();
        }

        logger.log("Provide security question: ");
        String securityQuestion = input.nextLine();
        logger.log("Provide answer to the question: ");
        String securityQuestionAnswer = input.nextLine();

        return new User(name, userName, password, securityQuestion, securityQuestionAnswer);
    }

    /**
     * checks if user already exists in the db
     * @param username
     * @return
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
     * encrypts the user password as a message digest
     * @param password
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static String encryptPassword(String password) throws NoSuchAlgorithmException{
        String encryptedPassword = password;
        MessageDigest MD5 = MessageDigest.getInstance(encryptionAlgorithm);
        MD5.update(encryptedPassword.getBytes());
        encryptedPassword = new String(MD5.digest());
        return encryptedPassword;
    }
}
