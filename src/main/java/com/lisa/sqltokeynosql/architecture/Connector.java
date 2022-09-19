package com.lisa.sqltokeynosql.architecture;

import com.lisa.sqltokeynosql.util.sql.Table;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import com.lisa.sqltokeynosql.util.operations.Operator;

import java.util.*;

import static java.util.stream.Collectors.toCollection;

/**
 * @author geomar
 */
public abstract class Connector {

    public abstract void connect(String nameDB);

    public void create(Table table) {}

    public void drop(Table table) {}

    public abstract void put (com.lisa.sqltokeynosql.util.Dictionary dictionary, Table table, String key, LinkedList<String> cols, ArrayList<String> values);

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

    protected boolean applyFilterR(List<Object> filters, HashMap tuple)
    {
        if (filters == null) {
            return true;
        }
        boolean result;
        Object o = filters.get(2);
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
            Object val = Parser.removeInvalidCaracteres(filters.get(1).toString());
            var op = ((Operator) o);
            var col = ((Column) filters.get(0)).getColumnName();
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
