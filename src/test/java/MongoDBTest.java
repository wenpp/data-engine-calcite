import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.List;

public class MongoDBTest {

    @Test
    public void SchemaMock(){
        //每个企业有一个schema

    }

    @Test
    public void MetaDataMock(){
        //初始化企业schema,获取了企业ID：SP00001
        CalciteSchema schema1 = CalciteSchema.createRootSchema(true,true,"SP00001");

        //获取元数据初始化Table
        //Product,Customer,Order,Order Item
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        //Product
        RelDataTypeFactory.Builder product = new RelDataTypeFactory.Builder(typeFactory);
        product.add("GUID", SqlTypeName.BIGINT);
        product.add("Name", SqlTypeName.VARCHAR);
        product.add("ProductNo", SqlTypeName.VARCHAR);
        product.add("ProductPrice", SqlTypeName.DOUBLE);
        product.add("Category", SqlTypeName.VARCHAR);
        product.add("Brand", SqlTypeName.VARCHAR);

        //row type  product.build()
//        MetaDataTable productTable = new MetaDataTable(product)
    }



    private static class MetaDataTable extends AbstractTable implements FilterableTable {
        RelDataTypeFactory.Builder type;


        public MetaDataTable(RelDataTypeFactory.Builder type) {
            this.type = type;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
            return null;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return type.build();
        }
    }

}
