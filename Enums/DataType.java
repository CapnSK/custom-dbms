package Enums;

public enum DataType {
    //Numeric Data types
    INT("INT"), DOUBLE("DOUBLE"),

    //Date and Time Data types
    DATE("DATE"), TIME("TIME"), DATETIME("DATETIME"),

    //String Data types
    VARCHAR("VARCHAR");

    private final String label;

    private DataType(String label){
        this.label = label;
    }

    public String getLabel(){
        return this.label;
    }

}
