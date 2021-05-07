import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

public class ViewTableMock {
    private static final List<Object[]> AUTHOR_DATA = Arrays.asList(
            new Object[]{0, "Victor", "Hugo",32},
            new Object[]{1, "Alexandre", "Dumas",15}
    );
    @Test
    public void viewTableMock(){
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        //1. Create the root schema describing the data model
        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        RelDataTypeFactory.Builder metaDataType = new RelDataTypeFactory.Builder(typeFactory);
        metaDataType.add("id", SqlTypeName.INTEGER);
        metaDataType.add("fname", SqlTypeName.VARCHAR);
        metaDataType.add("lname", SqlTypeName.VARCHAR);
        metaDataType.add("age", SqlTypeName.INTEGER);

        ListTable authorsTable = new ListTable(metaDataType.build(), AUTHOR_DATA);
//        public ViewTable(Type elementType, RelProtoDataType rowType, String viewSql,
//                List<String> schemaPath, List<String> viewPath) {
//            super(elementType);
//            this.viewSql = viewSql;
//            this.schemaPath = ImmutableList.copyOf(schemaPath);
//            this.protoRowType = rowType;
//            this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
//        }

//        ViewTable youngAuthor = new ViewTable(metaDataType.build(),metaDataType.build(),"",schema.getPath().,"");
    }

    private static class ListTable extends AbstractTable implements ScannableTable {
        private final RelDataType rowType;
        private final List<Object[]> data;

        ListTable(RelDataType rowType, List<Object[]> data) {
            this.rowType = rowType;
            this.data = data;
        }

        @Override public Enumerable<Object[]> scan(final DataContext root) {
            return Linq4j.asEnumerable(data);
        }

        @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
            return rowType;
        }
    }

}
