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
import java.util.concurrent.atomic.AtomicBoolean;
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


    
    /** 
     * @apiNote Stores user and credentials in db
     * @param user
     * @return Boolean
     */
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

    /**
     * @apiNote Use this API to get all the users in system
     * @return List of all User models
     */
    public static List<User> getAllUsers(){
        return getAllUsers(false);
    }

    /**
     * @apiNote Use this API to get all the users in system (or cache) if cache present
     * @param forceUpdate true means fetch from db else if cache is present return that value
     * @return
     */
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
                    if(!input.equals("\n") && !input.isEmpty()){
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
    
    /**
     * @apiNote Checks if user db exists or not
     * @param user
     * @return false if db for the user does not exist
     */
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

    /**
     * @apiNote returns the database present for the given user
     * @param user
     * @return
     */
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

    /**
     * @apiNote creates a database for user with given name
     * @param user
     * @param dbName
     * @return
     */
    public static Boolean createDB(User user, String dbName){
        Boolean dbCreated = false;;
        if(!dbExists(user)){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+dbName);
            file.mkdirs();
            dbCreated = file.exists();
        }
        return dbCreated;
    }

    /**
     * @apiNote creates a table with given table schema and for the give user
     * @param table
     * @param user
     * @return
     */
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
                    if(tableSchemaFile.exists()){
                        tableSchemaFile.delete();
                    }
                    if(!tableSchemaFile.exists()){
                        fileCreated = tableSchemaFile.createNewFile();
                    }
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
                                if(column.getConstraints() != null){
                                    fileWriter.append(Constants.FILE_DELIMETER);
                                    fileWriter.append(
                                        String.join(
                                            Constants.FILE_DELIMETER,
                                            column.getConstraints().stream().map(ConstraintType::getLabel).collect(Collectors.toList()))
                                    );
                                }
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

    /**
     * @apiNote fetches table schema from db
     * @param tableName
     * @param user
     * @return
     */
    public static Table fetchTableSchema(String tableName, User user){
        Table table = null;
        if(tableName != null && user.getUsername() != null && dbExists(user) && tableExists(tableName, user)){
            try(Scanner fileReader = new Scanner(new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".schema"))){
                List<Column> columns = new ArrayList<>();
                while(fileReader.hasNextLine()){
                    String input = fileReader.nextLine();
                    if(!input.equals("\n") && !input.isEmpty()){
                        List<String> columnInfo = Arrays.asList(input.split(Constants.FILE_DELIMETER.replace("$", "\\$"),3));
                        if(columnInfo.size() >= 2){
                            String columnName = columnInfo.get(0);
                            DataType columnType = DataType.valueOf(columnInfo.get(1));

                            List<String> rawConstraints = columnInfo.size() == 3 ? Arrays.asList(columnInfo.get(2).split(Constants.FILE_DELIMETER.replace("$", "\\$"), -1)) : null;
                            List<ConstraintType> constraints = rawConstraints != null ? rawConstraints.stream().map(c->{
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
                            }).filter(Objects::nonNull).collect(Collectors.toList()) : null;
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

    /**
     * @apiNote checks if table with given name exists for the user or not
     * @param tableName
     * @param user
     * @return
     */
    public static Boolean tableExists(String tableName, User user){
        Boolean tableExists = false;
        if(tableName != null && user.getUsername() != null && dbExists(user)){
            File file = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".schema");
            tableExists = file.exists() && file.isFile();
        }
        return tableExists;
    }

    /**
     * @apiNote fetches data from the table with given name and for given user
     * @param tableName
     * @param user
     * @return
     */
    public static List<List<String>> getTableData(String tableName, User user){
        List<List<String>> values = null;
        if(tableName != null && user != null && tableExists(tableName, user)){
            File dataFile = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".data");
            values = new ArrayList<>();
            if(dataFile.exists() && dataFile.isFile()){
                try(Scanner fileReader = new Scanner(dataFile)){
                    while(fileReader.hasNextLine()){
                        String input = fileReader.nextLine();
                        if(!input.equals("\n") && !input.isEmpty()){
                            values.add(Arrays.asList(input.split(Constants.FILE_DELIMETER.replace("$", "\\$"), -1)));
                        }
                    }
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        return values;
    }

    /**
     * Returns values in raw (string) format for one column
     * @param tableName
     * @param columnName
     * @return
     */
    public static List<String> getColumnData(String tableName, String columnName, User user){
        List<String> values = null;
        if(tableName != null && user != null && tableExists(tableName, user)){
            File dataFile = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tableName+".data");
            values = new ArrayList<>();
            Table table = fetchTableSchema(tableName, user);
            Integer indexOfColumnToFetch = -1;
            if(table != null && table.getColumns() != null){
                indexOfColumnToFetch = table.getColumns().stream().map(Column::getName).collect(Collectors.toList()).indexOf(columnName);
            }
            if(dataFile.exists() && dataFile.isFile() && indexOfColumnToFetch != -1){
                try(Scanner fileReader = new Scanner(dataFile)){
                    while(fileReader.hasNextLine()){
                        String input = fileReader.nextLine();
                        if(!input.equals("\n") && !input.isEmpty()){
                            List<String> row = Arrays.asList(input.split(Constants.FILE_DELIMETER.replace("$", "\\$"), -1));
                            if(indexOfColumnToFetch < row.size()){
                                values.add(row.get(indexOfColumnToFetch));
                            }
                        }
                    }
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        return values;
    }

    /**
     * @apiNote inserts data in table (rewrites completely if already present)
     * @param tablename
     * @param values
     * @param user
     * @return
     */
    public static Boolean insertData(String tablename, List<List<String>> values, User user){
        return insertData(tablename, values, user, false);
    }

    /**
     * @apiNote inserts data in table (rewrites completely if append is false)
     * @param tablename
     * @param values
     * @param user
     * @param append
     * @return
     */
    public static Boolean insertData(String tablename, List<List<String>> values, User user, Boolean append){
        /** 
         * Assumption
         * 1. This method is to insert values in bulk to the table
         * 2. It will append to the existing file if append is true
         * 3. When append is true, it is user's responsibility to provide values in sorted manner as per PRIMARY KEY
         */
        AtomicBoolean insertedSuccessfully = new AtomicBoolean(true);
        if(tablename != null && user != null && tableExists(tablename, user)){
            File dataFile = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tablename+".data");
            if(dataFile.getParentFile() != null && !dataFile.getParentFile().exists()){
                dataFile.getParentFile().mkdirs();
            }
            if(dataFile.getParentFile() != null && dataFile.getParentFile().exists() && !dataFile.exists()){
                Boolean fileCreated = false;
                try {
                    fileCreated = dataFile.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if(dataFile.exists()){
                try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(dataFile, append))){
                    values.stream().forEach(row->{
                        try {
                            fileWriter.append(String.join(Constants.FILE_DELIMETER,row));
                            fileWriter.append("\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                            insertedSuccessfully.set(false);
                        }
                    });
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        return insertedSuccessfully.get();
    }

    /**
     * @apiNote inserts given row in table
     * @param tablename
     * @param user
     * @param row
     * @return
     */
    public static Boolean insertRow(String tablename, User user, List<String> row){
        /** 
         * Assumptions
         * 1. Data is already sorted in file
         * 2. Adding new row means the id will always be in increasing order
         */
        Boolean insertedSuccessfully = true;
        if(tablename != null && user != null && tableExists(tablename, user)){
            File dataFile = new File(ROOT_DIR + DATA_DIR + user.getUsername()+"/"+getDBName(user)+"/"+tablename+".data");
            if(dataFile.getParentFile() != null && dataFile.getParentFile().exists() && !dataFile.exists()){
                Boolean fileCreated = false;
                try {
                    fileCreated = dataFile.createNewFile();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if(dataFile.exists() && dataFile.isFile()){
                try(BufferedWriter fileWriter = new BufferedWriter(new FileWriter(dataFile, true))){
                    fileWriter.append(String.join(Constants.FILE_DELIMETER,row));
                    fileWriter.append("\n");
                } catch(IOException e){
                    insertedSuccessfully = false;
                    e.printStackTrace();
                }
            }
        }
        return insertedSuccessfully;
    }
}
