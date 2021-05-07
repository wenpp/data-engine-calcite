package cc.infocloud.engine.csv;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

public class DataEngineTable extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }
}
