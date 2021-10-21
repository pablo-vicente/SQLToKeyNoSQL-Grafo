/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util.sql;

import java.util.Stack;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
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
public class WhereStatement extends ExpressionDeParser{
    public int i = 0;
    private Stack<Object> parsedFilters;

    public WhereStatement() {
        super();
        parsedFilters = new Stack();
    }
    
    @Override
    public void visit(NullValue nv) {
        super.visit(nv);
    }

    @Override
    public void visit(Function fnctn) {
        super.visit(fnctn);
    }

    @Override
    public void visit(SignedExpression se) {
        super.visit(se);
    }

    @Override
    public void visit(DoubleValue dv) {
        super.visit(dv);
        System.out.println((++i)+" "+dv);//To change body of generated methods, choose Tools | Templates.
        parsedFilters.push(Double.valueOf(dv.toString()));
    }

    @Override
    public void visit(LongValue lv) {
        super.visit(lv);
        parsedFilters.push(Integer.valueOf(lv.toString()));
        //System.out.println((++i)+" l "+lv); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(DateValue dv) {
        super.visit(dv);
        //System.out.println((++i)+" "+dv);
        parsedFilters.push(dv);
    }

    @Override
    public void visit(TimeValue tv) {
        super.visit(tv);
        //System.out.println((++i)+" "+tv);
        parsedFilters.push(tv);
    }

    @Override
    public void visit(TimestampValue tv) {
        super.visit(tv);
//System.out.println((++i)+" "+tv);
        parsedFilters.push(tv);
    }

    @Override
    public void visit(Parenthesis prnths) {
        super.visit(prnths);
        //System.out.println((++i)+" (");
       // System.out.println((++i)+" )");
    }

    @Override
    public void visit(StringValue sv) {
        super.visit(sv);
        //System.out.println(" "+(++i)+" "+sv);
        parsedFilters.push(sv.toString());
    }

    @Override
    public void visit(Addition adtn) {
        super.visit(adtn);
       // System.out.println(" "+(++i)+" "+"+");
        parsedFilters.push(adtn);
    }

    @Override
    public void visit(Division dvsn) {
        //System.out.println(" "+(++i)+" "+"\\"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Multiplication m) {
        //System.out.println(" "+(++i)+" "+"*"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Subtraction s) {
        //System.out.println(" "+(++i)+" "+"-"); //To change body of generated methods, choose Tools | Templates.
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

    @Override
    public void visit(SubSelect ss) {
        super.visit(ss);
    }

    @Override
    public void visit(CaseExpression ce) {
        super.visit(ce);
    }

    @Override
    public void visit(WhenClause wc) {
        super.visit(wc);
    }

    @Override
    public void visit(ExistsExpression ee) {
        super.visit(ee);
    }

    public Stack<Object> getParsedFilters() {
        return parsedFilters;
    }
    
}
