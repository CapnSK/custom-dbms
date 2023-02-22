package Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import Enums.ConstraintType;
import Enums.DDLCommandType;
import Enums.DMLCommandType;
import Model.internal.Column;
import Model.internal.Table;
import Utils.Constants;

public class QueryService {
    private static Scanner input = new Scanner(System.in);
    public static Boolean acceptQueries(){
        if(AuthenticationService.getActiveUser() != null){
            if(!PersistenceService.dbExists(AuthenticationService.getActiveUser())){
                System.out.println("Please provide a name for your database");
                String dbName = input.nextLine();
                Boolean dbCreated = PersistenceService.createDB(AuthenticationService.getActiveUser(), dbName);
                if(dbCreated){
                    System.out.println(dbName + " database created successfully");
                }
            }
            else {
                System.out.println("Selected database: "+PersistenceService.getDBName(AuthenticationService.getActiveUser()));
            }
            
            System.out.println("Enter query: ");
            String query = input.nextLine();
            while(!query.equalsIgnoreCase(Constants.EXIT_COMMAND)){                
                QueryService.resolveQueries(query);
                query = input.nextLine();
            }
        }
        return true;
    }

    private static void resolveQueries(String query){
        if(query != null && !query.isEmpty()){
            List<String> tokens = Arrays.asList(query.split(" ", -1));
            if(tokens.size() < 3){
                System.out.println("Invalid query please use correct syntax");
            }
            else {
                String command = tokens.get(0);
                if(Stream.of(DMLCommandType.values()).anyMatch(dmlCommand -> dmlCommand.getLabel().equalsIgnoreCase(command))){
                    //DML Command flow
                    switch(DMLCommandType.valueOf(command.toUpperCase())){
                        case INSERT:
                            QueryService.DML.handleInsertQuery(tokens);
                            break;
                        case DELETE:
                            QueryService.DML.handleDeleteQuery(tokens);
                            break;
                        case SELECT:
                            QueryService.DML.handleSelectQuery(tokens);
                            break;
                        case UPDATE:
                            QueryService.DML.handleUpdateQuery(tokens);
                            break;
                        default:
                            break;
                    }
                }
                else if(Stream.of(DDLCommandType.values()).anyMatch((ddlCommand) -> ddlCommand.getLabel().equalsIgnoreCase(command))){
                    //DDL Command flow
                    switch(DDLCommandType.valueOf(command.toUpperCase())){
                        case CREATE:
                            QueryService.DDL.handleCreateQuery(query, tokens);
                            break;
                        default:
                            break;
                    }
                }   
            }
        }
    }


    private static class DML{
        public static void handleInsertQuery(List<String> tokens){

        }

        public static void handleDeleteQuery(List<String> tokens){

        }

        public static void handleUpdateQuery(List<String> tokens){

        }

        public static void handleSelectQuery(List<String> tokens){
            /**
             * SELECT col1, col2...
             * FROM table1, table2
             * WHERE condition1 AND condition2 ...
             * GROUP BY colname
             * HAVING expression
             * ORDER BY expression
             * LIMIT count
             * 
             * execution order:
             * 1. FROM tablename
             * 2. JOIN / ON
             * 3. WHERE condition
             * 4. GROUP BY colname
             * 5. HAVING expression
             * 6. SELECT col1, col2 ...
             * 7. DISTINCT
             * 8. ORDER BY expression order
             * 9. LIMIT limit
             */

        }
    }

    private static class DDL{
        public static void handleCreateQuery(String query, List<String> tokens){
            /*
             * FORMAT: CREATE TABLE tablename (
             *              col1name col1type [constraints], i/
             *              col2name col2type [constraints], i/
             *              ...
             *              [constraints] X
             *         );
             * Assumption: There will be no table level constaints
             */

            // Query Validation as per format
            Boolean queryValid = validateCreateQuery(query, tokens);
            
            if(queryValid){
                List<Column> columns = extractColumns(query);
                String tableName = query.substring(0, query.indexOf("(")).split(" ", 3)[2];
                Table table = new Table(tableName, columns);
                Boolean tableCreated = PersistenceService.createTable(table);
                if(!tableCreated){
                    //provide error message
                }
            }
            else{
                //provide error message
            }
            
        }

        private static List<Column> extractColumns(String query){
            List<String> columns = Arrays.asList(query.substring(query.indexOf("("), query.lastIndexOf(")")).split(",", -1));
            return columns.stream().filter(Objects::nonNull).map(col -> {
                //colname coltype [constraints]
                List<String> tokens = Arrays.asList(col.stripLeading().split(" ", 3));
                Column column = null;
                if(tokens.size() <= 3){
                    column = new Column();
                    column.setName(tokens.get(0).replaceFirst("\\(", ""));
                    column.setDataType(tokens.get(1));
                    if(tokens.size() == 3){
                        column.setConstraints(retrieveConstraints(tokens.get(2)));
                    }
                }
                return column;
            }).collect(Collectors.toList());
        }

        private static List<ConstraintType> retrieveConstraints(String token){
            List<ConstraintType> constraints = null;
            if(token != null){
                constraints = new ArrayList<>();
                List<String> rawConstrainList = Arrays.asList(token.stripLeading().split(" ", -1));

                Integer iterator = 0;
                while(iterator < rawConstrainList.size()){
                    String tkn = rawConstrainList.get(iterator);
                    switch(tkn.toUpperCase()){
                        case "PRIMARY":
                            if((iterator + 1) < rawConstrainList.size() && rawConstrainList.get(iterator + 1).equalsIgnoreCase("KEY")){
                                constraints.add(ConstraintType.PRIMARY_KEY);
                                iterator+=1;
                            }
                            break;
                        case "NOT":
                            if((iterator + 1) < rawConstrainList.size() && rawConstrainList.get(iterator + 1).equalsIgnoreCase("NULL")){
                                constraints.add(ConstraintType.NOT_NULL);
                                iterator+=1;
                            }
                            break;
                        case "UNIQUE":
                            constraints.add(ConstraintType.UNIQUE);
                            break;
                        default:
                            break;
                    }
                    iterator+=1;
                }
            }
            return constraints;
        }

        private static Boolean validateCreateQuery(String query, List<String> tokens){
            Boolean queryValid = true;
            //1. check brackets
            queryValid = (query.contains("(") && query.contains(")") && (query.indexOf("(")<query.lastIndexOf(")")));
            //2. check if min 1 column provided
            queryValid = queryValid && (query.substring(query.indexOf("("), query.lastIndexOf(")")).split(",", -1).length >= 1);
            //3. find expected keywords
            queryValid = queryValid && (tokens.get(1).equalsIgnoreCase("TABLE"));
            return queryValid;
        }
    }
}
