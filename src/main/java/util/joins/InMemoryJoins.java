package util.joins;

import java.util.List;
import java.util.Stack;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import util.DataSet;
import util.operations.Operator;

/**
 *
 * @author geomar
 */
public abstract class InMemoryJoins {
    public abstract DataSet join(DataSet inner, DataSet outer, Stack join);

    protected boolean applyFilterR(Stack<Object> filters, List innerC, List outerC, String[] innerT, String[] outerT) {
        if (filters == null) {
            return true;
        }
        Boolean result = false;
        Object o = filters.pop();
        if (o instanceof AndExpression) {
            result = applyFilterR(filters, innerC, outerC, innerT, outerT);
            if (!result) {
                return false;
            }
            result = (result && applyFilterR(filters, innerC, outerC, innerT, outerT));
        } else if (o instanceof OrExpression) {
            result = applyFilterR(filters, innerC, outerC, innerT, outerT);
            result = (result || applyFilterR(filters, innerC, outerC, innerT, outerT));
        } else {
            String colI = null;
            String colO = null;
            Operator op = null;
            op = ((Operator) o);
            o = filters.pop();
            colO = ((Column) o).getTable().getName()+"."+((Column) o).getColumnName();
            o = filters.pop();
            colI = ((Column) o).getTable().getName()+"."+((Column) o).getColumnName();
            result = compare((String) innerT[innerC.indexOf(colI)], op, ((String) outerT[outerC.indexOf(colO)]));
        }
        return result;
    }
    
    private boolean compare(String v1, Operator operation, String val) {
        if (v1 == null || val == null) {
            return false;
        }
        return operation.compare(v1, val);
    }
}
