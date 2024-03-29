package Model.internal;

/**
 * @implNote This model is to store expressions of type age>18 or name="xyz" etc
 */
public class Expression {
    private String leftOperand; //columnName
    private String rightOperand; // value compatible with that column's data type
    private String operator; // valid comparison operator for that data type

    
    /** 
     * @return String
     */
    public String getLeftOperand() {
        return leftOperand;
    }
    public void setLeftOperand(String leftOperand) {
        this.leftOperand = leftOperand;
    }
    public String getRightOperand() {
        return rightOperand;
    }
    public void setRightOperand(String rightOperand) {
        this.rightOperand = rightOperand;
    }
    public String getOperator() {
        return operator;
    }
    public void setOperator(String operator) {
        this.operator = operator;
    }
}
