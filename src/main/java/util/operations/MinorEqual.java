package util.operations;

/**
 *
 * @author geomar
 */
public class MinorEqual extends Operator{

    @Override
    public boolean compare(Object a, Object b) {
        return (Double.valueOf(a.toString()))<=(Double.valueOf(b.toString()));
    }

}
