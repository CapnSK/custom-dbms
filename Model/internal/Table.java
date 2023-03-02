package Model.internal;

import java.util.List;

/**
 * @implNote this model represents table schema in memory
 */
public class Table {
    String name;
    List<Column> columns;

    public Table(){

    }

    public Table(String name, List<Column> columns){
        this.name = name;
        this.columns = columns;
    }

    
    /** 
     * @return String
     */
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<Column> getColumns() {
        return columns;
    }
    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
