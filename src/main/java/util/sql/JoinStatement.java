package util.sql;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import util.operations.*;

import java.util.Stack;

/**
 * @author geomar
 */
public class JoinStatement extends ExpressionDeParser {
    public int i = 0;
    private Stack<Object> parsedFilters;

    public JoinStatement() {
        super();
        parsedFilters = new Stack();
    }

    @Override
    public void visit(AndExpression ae) {
        super.visit(ae);
        parsedFilters.push(ae);
    }

    @Override
    public void visit(OrExpression oe) {
        super.visit(oe);
        parsedFilters.push(oe);
    }

    @Override
    public void visit(Between btwn) {
        super.visit(btwn);
    }

    @Override
    public void visit(EqualsTo et) {
        super.visit(et);
        parsedFilters.push(new Equal());
    }

    @Override
    public void visit(GreaterThan gt) {
        super.visit(gt);
        parsedFilters.push(new Greater());
    }

    @Override
    public void visit(GreaterThanEquals gte) {
        super.visit(gte);
        parsedFilters.push(new GreaterEqual());
    }

    @Override
    public void visit(InExpression ie) {
        super.visit(ie);
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
    }

    @Override
    public void visit(MinorThanEquals mte) {
        super.visit(mte);
        parsedFilters.push(new MinorEqual());
    }

    @Override
    public void visit(NotEqualsTo net) {
        net.getLeftExpression().accept(this);
        parsedFilters.push(new NotEqual());
        net.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        super.visit(column);
        parsedFilters.push(column);
    }

    public Stack<Object> getParsedFilters() {
        return parsedFilters;
    }
}
