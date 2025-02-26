package com.lisa.sqltokeynosql.architecture;

import com.lisa.sqltokeynosql.util.AlterDto;
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

    public abstract void put (Table table, List<String> cols, Map<String, List<String>> dados);

    public abstract void delete(String table, List<String> keys);

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

    public void update(Table table, HashMap<String, ArrayList<String>> dataSet, List<String> colunas, List<String> valores)
    {
        List<String> cols = table.getAttributes();
        for (Map.Entry<String, ArrayList<String>> stringArrayListEntry : dataSet.entrySet())
        {
            var key = stringArrayListEntry.getKey();
            var tuple = stringArrayListEntry.getValue();
            var array = new ArrayList<String>();
            array.add(key);
            delete(table.getName(), array);

            var put = new HashMap<String, List<String>>();
            put.put(key, tuple);
            put(table, cols, put);
        }
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

    public void alter(Table table, ArrayList<AlterDto> dados)
    {
    }
}
