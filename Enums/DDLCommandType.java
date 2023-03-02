package Enums;

/**
 * @implNote Data definition type commands allowed
 */
public enum DDLCommandType {
    CREATE("CREATE");
    //other sql DDL commands
    //ALTER, DROP, TRUNCATE, COMMENT, RENAME;   
    
    public final String label;

    private DDLCommandType(String label){
        this.label = label;
    }

    public String getLabel(){
        return this.label;
    }
}

