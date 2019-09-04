package util.SQL;

import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.operations.Equal;
import util.operations.Greater;
import util.operations.GreaterEqual;
import util.operations.Minor;
import util.operations.MinorEqual;
import util.operations.NotEqual;

/**
 *
 * @author geomar
 */
public class JoinStatment extends ExpressionDeParser{
     public int i = 0;
    private Stack<Object> parsedFilters;

    public JoinStatment() {
        super();
        parsedFilters = new Stack();        
    }

     @Override
    public void visit(AndExpression ae) {
        super.visit(ae);
       // System.out.println(" "+(++i)+" "+" and ");
        parsedFilters.push(ae);
    }

    @Override
    public void visit(OrExpression oe) {
        super.visit(oe);
        //System.out.println(" "+(++i)+" "+" or "); //To change body of generated methods, choose Tools | Templates.
        parsedFilters.push(oe);
    }

    @Override
    public void visit(Between btwn) {
        super.visit(btwn);
    }

    @Override
    public void visit(EqualsTo et) {
        super.visit(et);
        //System.out.println(" "+(++i)+" "+" = "); 
        parsedFilters.push(new Equal());
    }

    @Override
    public void visit(GreaterThan gt) {
        super.visit(gt);
        //System.out.println(" "+(++i)+" "+" > ");
        parsedFilters.push(new Greater());
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        super.visit(gte);
        parsedFilters.push(new GreaterEqual());
        //System.out.println(" "+(++i)+" "+" >= "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(InExpression ie) {
        super.visit(ie); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(IsNullExpression ine) {
        super.visit(ine);
    }

    @Override
    public void visit(LikeExpression le) {
        super.visit(le);
    }

    @Override
    public void visit(MinorThan mt) {
        super.visit(mt);
        parsedFilters.push(new Minor());
        //System.out.println(" "+(++i)+" "+" < "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(MinorThanEquals mte) {
        super.visit(mte);
        parsedFilters.push(new MinorEqual());
        //System.out.println(" "+(++i)+" "+" <= "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(NotEqualsTo net) {
        net.getLeftExpression().accept(this);
        parsedFilters.push(new NotEqual());
        net.getRightExpression().accept(this);
       // System.out.println(" "+(++i)+" "+" != "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Column column) {
        super.visit(column);
        parsedFilters.push(column);
        //System.out.println(" "+(++i)+" "+" col "+column.getColumnName()+" "); //To change body of generated methods, choose Tools | Templates.
    }
    public Stack<Object> getParsedFilters() {
        return parsedFilters;
    }
}
