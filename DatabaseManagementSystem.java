import java.util.Scanner;

import Enums.ApplicationState;
import Service.AuthenticationService;
import Service.LogService;
import Service.QueryService;
import Service.UserRegistrationService;

public class DatabaseManagementSystem {
    private static Scanner input = new Scanner(System.in);
    private static ApplicationState applicationState = ApplicationState.INIT; 
    private static LogService logger = LogService.getLogger(DatabaseManagementSystem.class);
    
    /** 
     * @apiNote This is a driver code for the dbm system. It maintains the application state and invokes the respective services
     * @param args
     */
    public static void main(String[] args){
        while(DatabaseManagementSystem.applicationState != ApplicationState.EXIT){
            switch(DatabaseManagementSystem.applicationState){
                case INIT:
                    Boolean existingUser = checkForExistingUser();
                    DatabaseManagementSystem.applicationState = existingUser ? ApplicationState.LOGIN :
                                                                                    ApplicationState.REGISTER;
                    break;
                case REGISTER:
                    Boolean userCreated = UserRegistrationService.registerUser();
                    DatabaseManagementSystem.applicationState = userCreated ? ApplicationState.LOGIN :
                                                                                    ApplicationState.REGISTER;
                    break;
                case LOGIN:
                    Boolean userLoggedIn = AuthenticationService.logInUser();
                    DatabaseManagementSystem.applicationState = userLoggedIn ?  ApplicationState.LOGGEDIN :
                                                                                    ApplicationState.LOGIN;
                    break;
                case LOGGEDIN:
                    Boolean exitApplication =  QueryService.acceptQueries();
                    if(exitApplication){
                        DatabaseManagementSystem.applicationState = ApplicationState.EXIT;
                    }
                    break;
                default:
                    DatabaseManagementSystem.applicationState = checkForExit() ? ApplicationState.EXIT : 
                                                                                    applicationState;
                    break;
            }
        }
        logger.log("Bye");
    }

    /**
     * @implNote checks for exit and returns true if needs exit
     * @return Boolean
     */
    private static Boolean checkForExit(){
        logger.log("Do you want to quit? (y/n)");
        String response = input.next();
        return response.equalsIgnoreCase("y");
    }

    
    /** 
     * @implNote checks if user should be logged in or new user 
     * @return Boolean
     */
    private static Boolean checkForExistingUser(){
        logger.log("Are you an existing user? (y/n)");
        String response = input.next();
        return response.equalsIgnoreCase("y");
    }
}