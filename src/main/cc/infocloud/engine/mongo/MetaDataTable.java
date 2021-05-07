package cc.infocloud.engine.mongo;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

public class MetaDataTable extends AbstractTable implements FilterableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }
}
