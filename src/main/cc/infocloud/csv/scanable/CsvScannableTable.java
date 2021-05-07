package cc.infocloud.csv.scanable;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

import java.util.concurrent.atomic.AtomicBoolean;

public class CsvScannableTable extends CsvTable
        implements ScannableTable {

    public CsvScannableTable(Source source, RelProtoDataType protoRowType) {
        super(source, protoRowType);
    }

    public String toString() {
        return "CsvScannableTable";
    }

    public Enumerable<Object[]> scan(DataContext root) {
        final int[] fields = CsvEnumerator.identityList(fieldTypes.size());
//        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        final AtomicBoolean cancelFlag = new AtomicBoolean(false);
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>(source, cancelFlag, false, null,
                        new CsvEnumerator.ArrayRowConverter(fieldTypes, fields));
            }
        };
    }
}
