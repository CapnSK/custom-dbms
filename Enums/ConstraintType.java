package Enums;

/**
 * @implNote These are the constraints (table/ colum level)
 */
public enum ConstraintType {
    PRIMARY_KEY("PRIMARY KEY"), NOT_NULL("NOT NULL"), UNIQUE("UNIQUE"), ;
    //more constraints
    // FOREIGN_KEY("FOREIGN KEY"), CHECK("CHECK"), DEFAULT("DEFAULT"), CREATE_INDEX("CREATE INDEX");

    private String label;

    private ConstraintType(String label){
        this.label = label;
    }

    public String getLabel(){
        return this.label;
    }
}
