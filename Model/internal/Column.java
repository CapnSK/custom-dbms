package Model.internal;

import java.util.List;

import Enums.ConstraintType;

public class Column {
    String name;
    String dataType;
    List<ConstraintType> constraints;
    
    public Column(){}
    
    public Column(String name, String dataType, List<ConstraintType> constraints){
        this.name = name;
        this.dataType = dataType;
        this.constraints = constraints;
    }
    
    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<ConstraintType> getConstraints() {
        return constraints;
    }
    public void setConstraints(List<ConstraintType> constraints) {
        this.constraints = constraints;
    }
    
}
