package com.lisa.sqltokeynosql.architecture;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import com.lisa.sqltokeynosql.util.operations.Operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Stack;

import static java.util.stream.Collectors.toCollection;

/**
 * @author geomar
 */
public abstract class Connector {

    public abstract void connect(String nameDB);

    public abstract void put(String table, String key, LinkedList<String> cols, ArrayList<String> values);

    public abstract void delete(String table, String key);

    public abstract HashMap<String, String> get(int n, String table, String key);

    public ArrayList getN(final int n, final String table, final ArrayList<String> keys, final Stack<Object> filters, final LinkedList<String> cols) {
        return keys
                .stream()
                .map(key -> get(n, table, key))
                .filter(tuple -> applyFilterR(filters, tuple))
                .map(tuple -> {
                    String[] tupleR = new String[cols.size()];
                    for (int i = 0; i < cols.size(); i++) {
                        tupleR[i] = tuple.get(cols.get(i));
                    }
                    return tupleR;
                })
                .collect(toCollection(ArrayList::new));
    }

    protected boolean applyFilterR(Stack<Object> filters, HashMap tuple) {
        if (filters == null) {
            return true;
        }
        boolean result;
        Object o = filters.pop();
        if (o instanceof AndExpression) {
            result = applyFilterR(filters, tuple);
            if (!result) {
                return false;
            }
            result = (result && applyFilterR(filters, tuple));
        } else if (o instanceof OrExpression) {
            result = applyFilterR(filters, tuple);
            result = (result || applyFilterR(filters, tuple));
        } else {
            String col = null;
            Object val = null;
            Operator op = null;
            op = ((Operator) o);
            val = filters.pop();
            col = ((Column) filters.pop()).getColumnName();
            result = compare((String) tuple.get(col), op, val);
        }
        return result;
    }

    protected boolean compare(String v1, Operator operation, Object val) {
        if (v1 == null || val == null) {
            return false;
        }
        return operation.compare(v1, val);
    }

}
