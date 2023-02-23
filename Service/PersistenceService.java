package Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import Enums.ConstraintType;
import Enums.DataType;
import Model.User;
import Model.internal.Column;
import Model.internal.Table;
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
                            .count() == 1;
            } else {
                dbFound = false;
            }
        }
        return dbFound;
    }

    public static String getDBName(User user){
        String dbName = null;
        if(user != null && user.getUsername() != null && dbExists(user)){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername());
            if(file.exists() && file.isDirectory()){
                dbName = Stream.of(file.listFiles())
                            .filter(File::isDirectory)
                            .map(File::getName).findFirst().orElse(null);
            }
        }
        return dbName;
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

    public static Boolean createTable(Table table, User user){
        Boolean tableCreated = false;
        if(table != null && table.getName() != null && table.getColumns() != null){
            //1. Create schema file as tablename.schema in physical structure
            String dbName = getDBName(user);
            String directoryName = ROOT_DIR + DATA_DIR + user.getUsername()+"/"+dbName;
            File parentDirectory = new File(directoryName);
            if(parentDirectory.exists() && parentDirectory.isDirectory()){
                String tableSchemaFileName = directoryName + "/"+table.getName()+".schema";
                File tableSchemaFile = new File(tableSchemaFileName);
                Boolean fileCreated = false;
                try {
                    fileCreated = tableSchemaFile.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                if(fileCreated){
                    //2. Write schema definition to file
                    try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tableSchemaFile))){
                        //table created successfully when all data written to file successfully
                        tableCreated = table.getColumns().stream().map(column -> {
                            Boolean fileWriteSuccesful = true;
                            try {
                                fileWriter.append(column.getName());
                                fileWriter.append(Constants.FILE_DELIMETER);
                                fileWriter.append(column.getDataType().getLabel());
                                fileWriter.append(Constants.FILE_DELIMETER);
                                fileWriter.append(
                                    String.join(
                                        Constants.FILE_DELIMETER,
                                        column.getConstraints().stream().map(ConstraintType::getLabel).collect(Collectors.toList()))
                                );
                                fileWriter.append("\n");
                            } catch (IOException e) {
                                fileWriteSuccesful = false;
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return fileWriteSuccesful;
                        }).allMatch(success->success);
                    } catch(IOException e){
                        e.printStackTrace();
                    }
                }
            }
            
        }
        return tableCreated;
    }

    public static Table fetchTableSchema(String tableName, User user){
        Table table = null;
        if(tableName != null && user.getUsername() != null && dbExists(user) && tableExists(tableName, user)){
            try(Scanner fileReader = new Scanner(new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".schema"))){
                List<Column> columns = new ArrayList<>();
                while(fileReader.hasNextLine()){
                    String input = fileReader.nextLine();
                    if(!input.equals("\n")){
                        List<String> columnInfo = Arrays.asList(input.split(Constants.FILE_DELIMETER.replace("$", "\\$"),3));
                        if(columnInfo.size() == 3){
                            String columnName = columnInfo.get(0);
                            DataType columnType = DataType.valueOf(columnInfo.get(1));

                            List<String> rawConstraints = Arrays.asList(columnInfo.get(2).split(Constants.FILE_DELIMETER.replace("$", "\\$"), -1));
                            List<ConstraintType> constraints = rawConstraints.stream().map(c->{
                                ConstraintType constraint = null;
                                switch(c){
                                    case "PRIMARY KEY":
                                        constraint = ConstraintType.PRIMARY_KEY;
                                        break;
                                    case "NOT NULL":
                                        constraint = ConstraintType.NOT_NULL;
                                        break;
                                    case "UNIQUE":
                                        constraint = ConstraintType.UNIQUE;
                                        break;
                                    default:
                                }
                                return constraint;
                            }).filter(Objects::nonNull).collect(Collectors.toList());
                            columns.add(new Column(columnName, columnType, constraints));
                        }
                    }
                }
                table = new Table(tableName, columns);
            } catch(IOException e){
                e.printStackTrace();
            }
        }
        return table;
    }

    public static Boolean tableExists(String tableName, User user){
        Boolean tableExists = false;
        if(tableName != null && user.getUsername() != null && dbExists(user)){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".schema");
            tableExists = file.exists() && file.isFile();
        }
        return tableExists;
    }
}
