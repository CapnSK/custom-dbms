package Model.internal;

import java.util.List;

import Enums.ConstraintType;
import Enums.DataType;

public class Column {
    String name;
    DataType dataType;
    List<ConstraintType> constraints;
    
    public Column(){}
    
    public Column(String name, DataType dataType, List<ConstraintType> constraints){
        this.name = name;
        this.dataType = dataType;
        this.constraints = constraints;
    }
    
    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
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
