/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
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
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
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
public class WhereStatment implements ExpressionVisitor{
    public int i = 0;
    private ArrayList<Object> parsedFilters;

    public WhereStatment() {
        parsedFilters = new ArrayList();
    }
    
    @Override
    public void visit(NullValue nv) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Function fnctn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(SignedExpression se) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(JdbcParameter jp) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(JdbcNamedParameter jnp) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(DoubleValue dv) {
        System.out.println((++i)+" "+dv);//To change body of generated methods, choose Tools | Templates.
        parsedFilters.add(Double.valueOf(dv.toString()));
    }

    @Override
    public void visit(LongValue lv) {
        parsedFilters.add(Integer.valueOf(lv.toString()));
        System.out.println((++i)+" l "+lv); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(DateValue dv) {
        System.out.println((++i)+" "+dv);
        parsedFilters.add(dv);
    }

    @Override
    public void visit(TimeValue tv) {
        System.out.println((++i)+" "+tv);
        parsedFilters.add(tv);
    }

    @Override
    public void visit(TimestampValue tv) {
        System.out.println((++i)+" "+tv);
        parsedFilters.add(tv);
    }

    @Override
    public void visit(Parenthesis prnths) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(StringValue sv) {
        System.out.println(" "+(++i)+" "+sv);
        parsedFilters.add(sv.toString());
    }

    @Override
    public void visit(Addition adtn) {
        System.out.println(" "+(++i)+" "+"+");
        adtn.getLeftExpression().accept(this);//To change body of generated methods, choose Tools | Templates.
        parsedFilters.add(adtn);
        adtn.getRightExpression().accept(this);
    }

    @Override
    public void visit(Division dvsn) {
        System.out.println(" "+(++i)+" "+"\\"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Multiplication m) {
        System.out.println(" "+(++i)+" "+"*"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Subtraction s) {
        System.out.println(" "+(++i)+" "+"-"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(AndExpression ae) {
        System.out.println(" "+(++i)+" "+" and ");
        ae.getLeftExpression().accept(this);
        parsedFilters.add(ae);
        ae.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression oe) {
        System.out.println(" "+(++i)+" "+" or "); //To change body of generated methods, choose Tools | Templates.
        oe.getLeftExpression().accept(this);
        parsedFilters.add(oe);
        oe.getRightExpression().accept(this);
    }

    @Override
    public void visit(Between btwn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(EqualsTo et) {
        System.out.println(" "+(++i)+" "+" = "); 
        et.getLeftExpression().accept(this);
        parsedFilters.add(new Equal());
        et.getRightExpression().accept(this);
    }

    @Override
    public void visit(GreaterThan gt) {
        System.out.println(" "+(++i)+" "+" > ");
        gt.getLeftExpression().accept(this);
        parsedFilters.add(new Greater());
        gt.getRightExpression().accept(this);//To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        gte.getLeftExpression().accept(this);
        parsedFilters.add(new GreaterEqual());
        gte.getRightExpression().accept(this);
        System.out.println(" "+(++i)+" "+" >= "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(InExpression ie) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(IsNullExpression ine) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(LikeExpression le) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(MinorThan mt) {
        mt.getLeftExpression().accept(this);
        parsedFilters.add(new Minor());
        mt.getRightExpression().accept(this);
        System.out.println(" "+(++i)+" "+" < "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(MinorThanEquals mte) {
        mte.getLeftExpression().accept(this);
            parsedFilters.add(new MinorEqual());
        mte.getRightExpression().accept(this);
        System.out.println(" "+(++i)+" "+" <= "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(NotEqualsTo net) {
        net.getLeftExpression().accept(this);
        parsedFilters.add(new NotEqual());
        net.getRightExpression().accept(this);
        System.out.println(" "+(++i)+" "+" != "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Column column) {
        parsedFilters.add(column);
        System.out.println(" "+(++i)+" "+" col "+column.getColumnName()+" "); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(SubSelect ss) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(CaseExpression ce) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(WhenClause wc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(ExistsExpression ee) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(AllComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(AnyComparisonExpression ace) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Concat concat) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Matches mtchs) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(BitwiseAnd ba) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(BitwiseOr bo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(BitwiseXor bx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(CastExpression ce) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(Modulo modulo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(AnalyticExpression ae) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(ExtractExpression ee) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(IntervalExpression ie) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(OracleHierarchicalExpression ohe) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void visit(RegExpMatchOperator remo) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public ArrayList<Object> getParsedFilters() {
        return parsedFilters;
    }
    
}
