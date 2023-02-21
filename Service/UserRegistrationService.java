package Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import Model.User;

public class UserRegistrationService {
    private static String encryptionAlgorithm = "MD5";

    private static Scanner input = new Scanner(System.in);
    public static Boolean registerUser(){
        User user = UserRegistrationService.createUserModel();
        try {
            user.setEncryptedPassword(UserRegistrationService.encryptPassword(user.getPassword()));
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return PersistenceService.storeUserCredentials(user);
    }

    private static User createUserModel(){
        System.out.println("Please enter your name: ");
        String name = input.nextLine();
        System.out.println("Please enter preferred username (should be unique): ");
        String userName = null;
        Boolean userNameTaken = true;
        do{
            userName = input.nextLine();
            userNameTaken = UserRegistrationService.checkIfUserExists(userName);
            if(userNameTaken){
                System.out.println("This user name is taken, please enter a different username: ");
            }
        } while(userNameTaken);

        System.out.println("Please enter your password: ");
        String password = input.nextLine();
        System.out.println("Re enter your password: ");
        String passwordCheck = input.nextLine();

        while(!password.equals(passwordCheck)){
            System.out.println("Both password do not match, please try again");
            passwordCheck = input.nextLine();
        }

        System.out.println("Provide security question: ");
        String securityQuestion = input.nextLine();
        System.out.println("Provide answer to the question: ");
        String securityQuestionAnswer = input.nextLine();

        return new User(name, userName, password, securityQuestion, securityQuestionAnswer);
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
        MessageDigest MD5 = MessageDigest.getInstance(encryptionAlgorithm);
        MD5.update(encryptedPassword.getBytes(), 0, 0);
        encryptedPassword = new String(MD5.digest(), StandardCharsets.UTF_8);
        return encryptedPassword;
    }
}
