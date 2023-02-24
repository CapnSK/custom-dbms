package Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Enums.ConstraintType;
import Enums.DDLCommandType;
import Enums.DMLCommandType;
import Enums.DataType;
import Model.internal.Column;
import Model.internal.Expression;
import Model.internal.Table;
import Utils.Constants;
import javafx.util.Pair;

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
                            QueryService.DML.handleInsertQuery(query);
                            break;
                        case DELETE:
                            QueryService.DML.handleDeleteQuery(tokens);
                            break;
                        case SELECT:
                            QueryService.DML.handleSelectQuery(query);
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
        public static void handleInsertQuery(String query){
            /*
             * FORMAT 1: INSERT INTO tablename (col1name, col2name, ...) VALUES (v1, v2, ...);
             * FORMAT 2: INSERT INTO tablename VALUES(v1, v2, ...);
             * Assumption: 
             */

            Boolean queryValid = validateInsertQuery(query);

            if(queryValid && query != null){
                List<Pair<Column, String>> values = new ArrayList<>();
                String tableName = null;
                //check if query is in format 1 or format 2 - If paranthesis starts before VALUES keyword
                if(query.indexOf("(") > query.toUpperCase().indexOf("VALUES")){
                    //FORMAT 2
                    //extract table name
                    tableName = query.split(" ", -1)[2];
                    final Table table = PersistenceService.fetchTableSchema(tableName, AuthenticationService.getActiveUser());
                    if(table != null && table.getColumns() != null){
                        String rawValues = query.substring(query.toUpperCase().indexOf("VALUES")+7);//len("VALUES") + 1

                        List<String> tokenizedValues = mapValues(rawValues);
                        if(tokenizedValues != null && tokenizedValues.size() == table.getColumns().size()){
                            IntStream.range(0, tokenizedValues.size()).forEach(index -> {
                                values.add(new Pair<>(
                                    table.getColumns().get(index), 
                                    tokenizedValues.get(index))
                                );
                            });
                        }
                    }

                }
                else{
                    //FORMAT 1
                    tableName = query.split(" ", -1)[2];
                    tableName = tableName.replaceAll("\\((.)*", "").strip();
                    final Table table = PersistenceService.fetchTableSchema(tableName, AuthenticationService.getActiveUser());
                    
                    if(table != null && table.getColumns() != null){
                        String rawValues = query.substring(query.toUpperCase().indexOf("VALUES")+7);
                        List<String> tokenizedValues = mapValues(rawValues);

                        String rawColumnNames = query.substring(query.indexOf("("), query.indexOf(")"));
                        List<String> tokenizedCols = mapValues(rawColumnNames);

                        if(tokenizedCols != null && tokenizedValues != null && tokenizedCols.size() == tokenizedValues.size()){
                            IntStream.range(0, tokenizedCols.size()).forEach(index->{
                                String colName = tokenizedCols.get(index);
                                Column column = table.getColumns().stream().filter(col -> col.getName().equals(colName)).findFirst().orElse(null);
                                if(column != null){
                                    values.add(new Pair<>(column, tokenizedValues.get(index)));
                                }
                            });
                        }
                    }
                }
                //Validate values against column constraints
                Table table = PersistenceService.fetchTableSchema(tableName, AuthenticationService.getActiveUser());
                Boolean valid = validateValues(table, values);

                if(valid){
                    List<String> storeValues = values.stream().map(Pair::getValue).collect(Collectors.toList());
                    //Insert values because they are valid
                    Boolean insertSuccessful = PersistenceService.insertRow(tableName, AuthenticationService.getActiveUser(), storeValues);
    
                    if(insertSuccessful){
                        System.out.println("Successfully inserted values in table");
                    }
                    else{
                        System.out.println("Could not insert values in the table. Please try again.");
                    }
                }
                else{
                    System.out.println("Values do not match the criteria. Please try again.");
                }


            }
            else{
                System.out.println("The query you have entered is invalid. Please try again. \nRefer correct schema here: https://www.tutorialspoint.com/sql/sql-insert-query.htm");
            }

        }

        private static Boolean validateValues(Table table, List<Pair<Column, String>> values){
            Boolean isValid = false;
            if(values != null){
                //1. Check data type and values
                isValid = values.stream().map(pair->{
                    return dataIntegrity(pair.getKey(), pair.getValue());
                }).allMatch(valid -> valid);
                
                //2. Check constraints and validate
                isValid = isValid && values.stream().map(pair->{
                    return constraintIntegrity(table, pair.getKey(), pair.getValue());
                }).allMatch(valid->valid);
            }
            return isValid;
        }

        private static Boolean dataIntegrity(Column column, String value){
            Boolean dataValid = true;
            switch(column.getDataType()){
                case INT:
                    try{
                        Integer.valueOf(value);
                    } catch(NumberFormatException e){
                        e.printStackTrace();
                        dataValid = false;
                    }
                    break;
                case DOUBLE:
                    try{
                        Double.valueOf(value);
                    } catch(NumberFormatException e){
                        e.printStackTrace();
                        dataValid = false;
                    }
                    break;
                case VARCHAR:
                    //no check required already a string
                    break;
                case DATE:
                    SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.SQL_DEFAULT_DATE_FORMAT);
                    try{
                        dateFormat.format(value);
                    } catch( IllegalArgumentException e){
                        e.printStackTrace();
                        dataValid = false;
                    }
                    break;
                case TIME:
                    SimpleDateFormat timeFormat = new SimpleDateFormat(Constants.SQL_DEFAULT_TIME_FORMAT);
                    try{
                        timeFormat.format(value);
                    } catch( IllegalArgumentException e){
                        e.printStackTrace();
                        dataValid = false;
                    }
                    break;
                case DATETIME:
                    SimpleDateFormat datetimeFormat = new SimpleDateFormat(Constants.SQL_DEFAULT_DATETIME_FORMAT);
                    try{
                        datetimeFormat.format(value);
                    } catch( IllegalArgumentException e){
                        e.printStackTrace();
                        dataValid = false;
                    }
                    break;
                default:
                    break;
            }
            return dataValid;
        }

        private static Boolean constraintIntegrity(Table table, Column column, String value){
            Boolean constraintIntegrityValid = true;
            if(table != null && column != null && value != null && column.getConstraints() != null && !column.getConstraints().isEmpty()){
                if(column.getConstraints().contains(ConstraintType.PRIMARY_KEY)){
                    column.getConstraints().remove(ConstraintType.PRIMARY_KEY);

                    column.getConstraints().add(ConstraintType.NOT_NULL);
                    column.getConstraints().add(ConstraintType.UNIQUE);
                }
                constraintIntegrityValid = column.getConstraints().stream().map(constraint ->{
                    Boolean constraintValid = false;
                    switch(constraint){
                        case UNIQUE:
                            List<String> values = PersistenceService.getColumnData(table.getName(), column.getName(),AuthenticationService.getActiveUser());
                            constraintValid = values.indexOf(value) == -1;
                            break;
                        case NOT_NULL:
                            constraintValid = value != null;
                            break;
                        default:
                    }
                    return constraintValid;
                }).allMatch(valid->valid);
            }
            return constraintIntegrityValid;
        }

        // "(val1, val2, val3)" -> [val1, val2, val3]
        private static List<String> mapValues(String token){
            List<String> values = new ArrayList<>();
            //remove empty white spaces
            token = token.strip();
            //remove parantheses at start
            token = token.replaceFirst("\\(", "");
            //remove parantheses & empty space at the end
            token = token.replaceAll("\\s*\\)\\s*;$", "");
            //split based on , but ensure that if string has a comma do not split there
            //use RegExp approach to split
            Pattern pattern = Pattern.compile(Constants.COMMA_SEPARATED_REGEXP);
            Matcher matcher = pattern.matcher(token);

            while(matcher.find()){
                values.add(matcher.group());
            }

            //strip individual values of spaces
            values = values.stream().map(String::strip).collect(Collectors.toList());
            
            return values;
        }

        private static Boolean validateInsertQuery(String query){
            Boolean queryValid = false;
            if(query != null){
                //check bracket positioning
                queryValid = query.contains("(") && query.contains(")") && query.indexOf("(") < query.indexOf(")");
                //check for keywords format
                queryValid = queryValid && (query.toUpperCase().contains("INTO") && query.toUpperCase().contains("VALUES"));
            }
            return queryValid;
        }




        public static void handleDeleteQuery(List<String> tokens){

        }

        public static void handleUpdateQuery(List<String> tokens){

        }



        public static void handleSelectQuery(String query){
            /**
             * Normal SQL query -
             * 
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
             * 
             * Scope of this query:
             * SELECT colNames FROM tableName [WHERE condition1 [OPERATOR condition2]];
             */

             Boolean queryValid = validateSelectQuery(query);

            if(queryValid){
                Pair<List<Expression>, String> parsedExpressions = null;
                //FROM tableName
                String tableName = (query.toUpperCase().contains("WHERE") ? 
                        query.substring(query.toUpperCase().indexOf("FROM")+5, query.toUpperCase().indexOf("WHERE"))
                        : query.substring(query.toUpperCase().indexOf("FROM")+5, query.toUpperCase().indexOf(";"))).strip();
                Table table = PersistenceService.fetchTableSchema(tableName, AuthenticationService.getActiveUser());
                //WHERE Condition
                String rawConditions = "";
                if(query.toUpperCase().contains("WHERE")){
                    rawConditions = query.substring(query.toUpperCase().indexOf("WHERE")+5).strip();
                    parsedExpressions = parseWhereClause(rawConditions);
                }
                List<List<String>> data = PersistenceService.getTableData(tableName, AuthenticationService.getActiveUser());

                //prune data based on where clause
                List<List<String>> prunedData = applyWhereClause(data, parsedExpressions, table.getColumns());

                //SELECT col1, col2 ...
                List<String> columnNames = query.contains("*") ? Arrays.asList("*") 
                                            : Arrays.asList(query.substring(query.toUpperCase().indexOf("SELECT")+6, query.toUpperCase().indexOf("FROM")).strip().split(",\\s*", -1));
                

                
                List<Integer> indicesToFetch = new ArrayList<>();
                if(table != null && table.getColumns() != null && !columnNames.get(0).equals("*")){
                    indicesToFetch = IntStream.range(0, table.getColumns().size()).map(index->{
                        Column column = table.getColumns().get(index);
                        if(columnNames.contains(column.getName())){
                            return index;
                        }
                        return -1;
                    }).filter(ind -> ind != -1).boxed().collect(Collectors.toList());
                }
                
                List<String> output = new ArrayList<>();
                //Add header row
                if(columnNames.get(0).equals("*") && table != null && table.getColumns() != null){
                    output.add(String.join(" ", table.getColumns().stream().map(Column::getName).collect(Collectors.toList())));
                } else{
                    output.add(String.join(" ",columnNames));
                }
                final List<Integer> indicesToFetchFinal = indicesToFetch;
                if(prunedData != null && !prunedData.isEmpty()){
                    prunedData.stream().forEach(row->{
                        if(columnNames.get(0).equals("*")){
                            output.add(String.join(" ",row));
                        }
                        else{
                            List<String> prunedRow = indicesToFetchFinal.stream().map(row::get).collect(Collectors.toList());
                            output.add(String.join(" ", prunedRow));
                        }
                    });
                }

                if(!output.isEmpty()){
                    System.out.println(String.join("\n", output));
                }
            } else{
                System.out.println("The query you have entered is invalid. Please try again. \nRefer correct schema here: https://www.tutorialspoint.com/sql/sql-select-query.htm");
            }
            
        }

        private static List<List<String>> applyWhereClause(List<List<String>> originalData, Pair<List<Expression>, String> conditions, List<Column> columns){
            List<List<String>> prunedData = originalData;
            if(conditions != null && conditions.getKey() != null && !conditions.getKey().isEmpty() && columns != null){
                Expression expression1 = conditions.getKey().get(0);
                Expression expression2 = conditions.getKey().size() == 2 ? conditions.getKey().get(1) : null;

                Column column1 = columns.stream().filter(column->column.getName().equals(expression1.getLeftOperand())).findFirst().orElse(null);
                Column column2 = expression2 != null ? columns.stream().filter(column->column.getName().equals(expression2.getLeftOperand())).findFirst().orElse(null) : null;

                prunedData = IntStream.range(0, originalData.size()).filter(rowIdx -> {
                    List<String> row = originalData.get(rowIdx);
                    Integer colInd1 = IntStream.range(0, columns.size()).filter(index->columns.get(index).getName().equals(expression1.getLeftOperand())).findFirst().orElse(-1);
                    Integer colInd2 = column2 != null ? IntStream.range(0, columns.size()).filter(index->columns.get(index).getName().equals(expression2.getLeftOperand())).findFirst().orElse(-1) : null;
                    String fieldVal1 = colInd1 != -1 ? row.get(colInd1) : null;
                    String fieldVal2 = colInd2 != null && colInd2 != -1 ? row.get(colInd2) : null;
                    Boolean include = evaluateCondition(fieldVal1, expression1, column1);
                    if(expression2 != null){
                        Boolean include2 = evaluateCondition(fieldVal2, expression2, column2);
                        switch(conditions.getValue()){
                            case "AND":
                                include = include && include2;
                                break;
                            case "OR":
                                include = include || include2;
                                break;
                            default:
                                break;
                        }
                    }
                    return include;
                }).boxed().map(originalData::get).collect(Collectors.toList());
            }
            return prunedData;
        }

        private static Boolean evaluateCondition(
            String actualValue, 
            Expression expression,
            Column column
        ){
            Boolean passed = false;
            if(actualValue != null && expression != null && column != null){
                String valueToCompare = expression.getRightOperand();
                String comparator = expression.getOperator();
                

                switch(column.getDataType()){
                    //only comparator allowed is = & !=
                    case DATE:
                    case DATETIME:
                    case TIME:
                    case VARCHAR:
                        switch(comparator){
                            case "=":
                                passed = valueToCompare.equals(actualValue);
                                break;
                            case "!=":
                                passed = !valueToCompare.equals(actualValue);
                                break;
                            default:
                                System.out.println("Invalid WHERE clause comparison");
                                break;
                        }
                        break;
                    //comparators allowed are <,>,<=,>=,=,!=
                    case DOUBLE:
                        Double valueToCompareParsedDouble  = Double.valueOf(valueToCompare);
                        Double actualValueParsedDouble  = Double.valueOf(actualValue);

                        Integer comparison = actualValueParsedDouble.compareTo(valueToCompareParsedDouble);
                        switch(comparator){
                            case "=":
                                passed = comparison == 0;
                                break;
                            case "!=":
                                passed = comparison != 0;
                                break;
                            case ">":
                                passed = comparison > 0;
                                break;
                            case "<":
                                passed = comparison < 0;
                                break;
                            case "<=":
                                passed = comparison <= 0;
                                break;
                            case ">=":
                                passed = comparison >= 0;
                                break;
                            default:
                                System.out.println("Invalid WHERE clause comparison");
                                break;
                        }
                        break;
                    case INT:
                        Integer valueToCompareParsedInt  = Integer.valueOf(valueToCompare);
                        Integer actualValueParsedInt  = Integer.valueOf(actualValue);
                        /**
                         * 0 equal 
                         * negative actual<compared 
                         * positive actual>compared
                         */
                        comparison = actualValueParsedInt.compareTo(valueToCompareParsedInt);
                        switch(comparator){
                            case "=":
                                passed = comparison == 0;
                                break;
                            case "!=":
                                passed = comparison != 0;
                                break;
                            case ">":
                                passed = comparison > 0;
                                break;
                            case "<":
                                passed = comparison < 0;
                                break;
                            case "<=":
                                passed = comparison <= 0;
                                break;
                            case ">=":
                                passed = comparison >= 0;
                                break;
                            default:
                                System.out.println("Invalid WHERE clause comparison");
                                break;
                        }
                        break;
                }

            }
            return passed;
        }

        /**
         * LHS operator RHS AND/OR LHS operator RHS
         * @param rawCondition
         * @return
         */
        private static Pair<List<Expression>, String> parseWhereClause(String rawCondition){
            Pair<List<Expression>, String> expressions = null;
            if(rawCondition != null){
                List<String> splitConditions = Arrays.asList(rawCondition.replaceAll(Constants.BRACKETS_REGEXP, "").replaceAll(";","").split("(OR|AND)", 2)).stream().map(String::strip).collect(Collectors.toList());
                List<Expression> exps = new ArrayList<>();
                splitConditions.stream().forEach(condition->{
                    Expression expression = new Expression();
                    expression.setLeftOperand(condition.split(Constants.OPERATORS_REGEXP,2)[0].strip());
                    expression.setRightOperand(condition.split(Constants.OPERATORS_REGEXP,2)[1].strip());
                    
                    if(splitConditions.get(0).contains(">=")){
                        expression.setOperator(">=");
                    }
                    else if(splitConditions.get(0).contains(">")){
                        expression.setOperator(">");
                    }
                    else if(splitConditions.get(0).contains("<=")){
                        expression.setOperator("<=");
                    }
                    else if(splitConditions.get(0).contains("<")){
                        expression.setOperator("<");
                    }
                    else if(splitConditions.get(0).contains("!=")){
                        expression.setOperator("!=");
                    }
                    else if(splitConditions.get(0).contains("=")){
                        expression.setOperator("=");
                    }
                    // expression.setOperator(splitConditions.get(0).f);
                    // Pattern pattern = Pattern.compile(Constants.OPERATORS_REGEXP);
                    // Matcher matcher = pattern.matcher(condition);
                    // if(matcher.find()){
                    // }
                    exps.add(expression);
                });

                expressions = new Pair<List<Expression>,String>(
                    exps, 
                    exps.size() > 1 ? 
                            rawCondition.toUpperCase().contains("AND") ? "AND" : "OR" 
                        : ""
                );

            }
            return expressions;
        }

        private static Boolean validateSelectQuery(String query){
            Boolean queryValid = false;
            if(query != null && !query.isEmpty()){
                //check for keywords
                queryValid = query.toUpperCase().contains("FROM");
                //select columns should be all or at least 1
                queryValid = queryValid && (query.contains("*") || query.substring(query.toUpperCase().indexOf("SELECT")+7, query.toUpperCase().indexOf("FROM")).strip().split(",\\s*", -1).length >= 1);
            }
            return queryValid;
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
                if(columns != null){
                    String tableName = query.substring(0, query.indexOf("(")).split(" ", 3)[2];
                    Table table = new Table(tableName, columns);
                    Boolean tableCreated = PersistenceService.createTable(table, AuthenticationService.getActiveUser());
                    if(!tableCreated){
                        System.out.println("There was an error creating the table. Please try again");
                    }
                    else {
                        System.out.println(tableName+" was created successfully");
                    }
                }
                else{
                    System.out.println("The query you have entered is invalid. Please try again. \nRefer correct schema here: https://www.tutorialspoint.com/sql/sql-create-table.htm");
                }
            }
            else{
                System.out.println("The query you have entered is invalid. Please try again. \nRefer correct schema here: https://www.tutorialspoint.com/sql/sql-create-table.htm");
            }
            
        }

        private static List<Column> extractColumns(String query){
            List<String> columns = Arrays.asList(query.substring(query.indexOf("("), query.lastIndexOf(")")).split(",", -1));
            AtomicBoolean wrongColumnDefinition = new AtomicBoolean(false);
            List <Column> mappedColumns = columns.stream().filter(Objects::nonNull).map(col -> {
                //colname coltype [constraints]
                List<String> tokens = Arrays.asList(col.stripLeading().split(" ", 3));
                Column column = null;
                if(tokens.size() >= 2 && tokens.size() <= 3){
                    column = new Column();
                    column.setName(tokens.get(0).replaceFirst("\\(", ""));
                    column.setDataType(mapDataType(tokens.get(1)));
                    if(tokens.size() == 3){
                        column.setConstraints(retrieveConstraints(tokens.get(2)));
                    }
                }
                else{
                    wrongColumnDefinition.set(true);
                }
                return column;
            }).collect(Collectors.toList());
            if(wrongColumnDefinition.get()){
                mappedColumns = null;
            }
            return mappedColumns;
        }

        private static DataType mapDataType(String rawType){
            DataType dataType = null;
            if(rawType != null){
                if(rawType.toUpperCase().contains("VARCHAR")){
                    dataType = DataType.VARCHAR;
                }
                else{
                    dataType = DataType.valueOf(rawType);
                }
            }
            return dataType;
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
            queryValid = (query.contains("(") && query.contains(")") && (query.indexOf("(")<query.indexOf(")")));
            //2. check if min 1 column provided 
            queryValid = queryValid && (query.substring(query.indexOf("("), query.lastIndexOf(")")).strip().length()>1);
            //3. find expected keywords
            queryValid = queryValid && (tokens.get(1).equalsIgnoreCase("TABLE"));
            return queryValid;
        }
    }
}
