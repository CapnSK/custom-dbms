package Enums;

/**
 * @implNote These are the data manipulation commands allowed in this DBMS
 */
public enum DMLCommandType {
    SELECT("SELECT"), INSERT("INSERT"), UPDATE("UPDATE"), DELETE("DELETE");
    //other sql DML commands
    //MERGE("MERGE"), CALL("CALL"), EXPLAIN_PLAN("EXPLAIN PLAN"), LOCK_TABLE("LOCK TABLE");

    public final String label;

    private DMLCommandType(String label){
        this.label = label;
    }

    public String getLabel(){
        return this.label;
    }
}
