package Utils;

public class Constants {
    public final static String FILE_DELIMETER = "_$_";
    public final static String EXIT_COMMAND = "Exit";
    //REFERENCE: https://stackoverflow.com/questions/11456850/split-a-string-by-commas-but-ignore-commas-within-double-quotes-using-javascript
    public final static String COMMA_SEPARATED_REGEXP = "(\".*?\"|'.*?'|[^\",\\s]+)(?=\\s*,|\\s*$)";
    public static final String OPERATORS_REGEXP = "(<|>|<=|>=|=|!=){1}";
    public static final String BRACKETS_REGEXP = "(\\(|\\))";
    public static final String DELETE_QUERY_REGEXP = "DELETE FROM ([a-zA-Z0-9]+) WHERE ([a-zA-Z]+)[\\s]*(<|>|>=|<=|=|!=){1}[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+)( (AND|OR) ([a-zA-Z]+)[\\s]*(<|>|>=|<=|=|!=){1}[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+))?;";

    public static final String UPDATE_QUERY_REGEXP = "UPDATE ([a-zA-Z0-9]+) SET (([a-zA-Z]+)[\\s]*=[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+)([\\s]*,[\\s]*([a-zA-Z]+)[\\s]*=[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+))*) WHERE ([a-zA-Z]+)[\\s]*(<|>|>=|<=|=|!=){1}[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+)( (AND|OR) ([a-zA-Z]+)[\\s]*(<|>|>=|<=|=|!=){1}[\\s]*(('|\")[a-zA-Z0-9\\s]+('|\")|[0-9]+))?;";

    public final static String SQL_DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public final static String SQL_DEFAULT_TIME_FORMAT = "HH-mm-ss";
    public final static String SQL_DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH-mm-ss";

    private Constants(){

    }
}
