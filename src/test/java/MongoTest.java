import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.Sources;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

public class MongoTest {

    @Test
    public void testMongoInit(){
        Connection connection = null;
        Statement statement = null;
        String model = "model-view-mongo";

        Properties info = new Properties();
        info.put("model", jsonPath(model));
        info.put(CalciteConnectionProperty.CASE_SENSITIVE,"false");

        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();


            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            List<String> path = Lists.newArrayList("mongo");

//            final List<String> corpPath = ImmutableList.<String>builder().addAll(path).add("SP1").ad.build();


            final List<String> viewPath1 = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
            calciteConnection.getRootSchema().add("PRODUCT", ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT _MAP['GUID'] as GUID,_MAP['OrgID'] as OrgID,_MAP['ObjID'] as ObjID,_MAP['Name'] as Name,_MAP['ProductNo'] as ProductNo,_MAP['ProductPrice'] as ProductPrice,_MAP['ProductStatus'] as ProductStatus,_MAP['Category'] as Category,_MAP['Brand'] as Brand FROM \"data\" WHERE _MAP['ObjID'] = 'object001'",path,viewPath1,false));

            final List<String> viewPath2 = ImmutableList.<String>builder().addAll(path).add("CUSTOMER").build();
            calciteConnection.getRootSchema().add("CUSTOMER", ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT _MAP['GUID'] as GUID,_MAP['OrgID'] as OrgID,_MAP['ObjID'] as ObjID,_MAP['Name'] as Name,_MAP['CustomerNo'] as CustomerNo,_MAP['FirstName'] as FirstName,_MAP['LastName'] as LastName,_MAP['NickName'] as NickName,_MAP['LoginName'] as LoginName,_MAP['CustomerStatus'] as CustomerStatus FROM \"data\" WHERE _MAP['ObjID'] = 'object002'",path,viewPath2,false));

            final List<String> viewPath3 = ImmutableList.<String>builder().addAll(path).add("ORDERS").build();
            calciteConnection.getRootSchema().add("ORDERS", ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT _MAP['GUID'] as GUID,_MAP['OrgID'] as OrgID,_MAP['ObjID'] as ObjID,_MAP['OrderNo'] as OrderNo,_MAP['CustomerNo'] as CustomerNo,_MAP['OrderStatus'] as OrderStatus,_MAP['OrderTime'] as OrderTime FROM \"data\" WHERE _MAP['ObjID'] = 'object003'",path,viewPath3,false));

            final List<String> viewPath4 = ImmutableList.<String>builder().addAll(path).add("ORDERITEMS").build();
            calciteConnection.getRootSchema().add("ORDERITEMS", ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT _MAP['GUID'] as GUID,_MAP['OrgID'] as OrgID,_MAP['ObjID'] as ObjID,_MAP['OrderNo'] as OrderNo,_MAP['ProductNo'] as ProductNo,_MAP['ItemPrice'] as ItemPrice,_MAP['ItemQuanity'] as ItemQuanity,_MAP['OrderItemStatus'] as OrderItemStatus FROM \"data\" WHERE _MAP['ObjID'] = 'object004'",path,viewPath4,false));

//            String sql = "select t1.Name,t2.OrderStatus from CUSTOMER t1, ORDERS t2 where t1.CustomerNo = t2.CustomerNo and t2.ORDERSTATUS='Completed'";
            String sql = "select t1.NickName,t2.OrderNo from CUSTOMER as t1 join ORDERS as t2 on t1.CustomerNo = t2.CustomerNo and t1.CustomerNo='Customer0003'";
//            String sql = "select t2.Name,t2.CustomerNo,t3.OrderNo,t1.Name as ProductName,t1.ProductPrice,t4.ItemPrice from Product t1,Customer t2,ORDERS t3,ORDERITEMS t4 where t4.ProductNo = t1.ProductNo and t3.OrderNo = t4.OrderNo and t2.CustomerNo = t3.CustomerNo and t2.Name='Cheng Yan'";
//            String sql = "select * from ORDERITEMS where ItemPrice>6000";

            //display execute sql
            Hook.QUERY_PLAN.addThread((Consumer<Object>) s-> System.out.println("Execute sql:"+s));

            final ResultSet resultSet = statement.executeQuery(sql);

            System.out.println(getData(resultSet));
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        return Sources.of(CsvTest.class.getResource("/" + path)).file().getAbsolutePath();
    }

    public static List<Map<String,Object>> getData(ResultSet resultSet)throws Exception{
        List<Map<String,Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }
}
