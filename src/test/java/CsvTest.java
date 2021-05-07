import com.alibaba.fastjson.JSON;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteRootSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.model.JsonView;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Sources;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class CsvTest {


    @Test
    public void testModelViewData() {
        Connection connection = null;
        Statement statement = null;
        String model = "model-view-data";

        Properties info = new Properties();
        info.put("model", jsonPath(model));

        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
//            String sql = "SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'";
            String sql = "select t2.NAME,t2.CustomerNo,t3.OrderNo,t1.NAME as ProductName,t1.ProductPrice,t4.ItemPrice from Product t1,Customer t2,ORDERS t3,ORDERITEM t4 where t4.ProductNo = t1.ProductNo and t3.OrderNo = t4.OrderNo and t2.CustomerNo = t3.CustomerNo and t2.NAME='Cheng Yan' ";
            //explain
//            printSqlExplain(connection,sql);


            final ResultSet resultSet = statement.executeQuery(sql);


            //data result
            System.out.println(getData(resultSet));
//            System.out.println(JSON.toJSONString(getData(resultSet)));
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testModelViewDataByDefine() {
        Connection connection = null;
        Statement statement = null;
        String model = "model-view-bydefine";

        Properties info = new Properties();
        info.put("model", jsonPath(model));

        try {

            connection = DriverManager.getConnection("jdbc:calcite:", info);
//            connection.unwrap(CalciteConnection.class).getRootSchema()

//            SchemaPlus schema = connection.unwrap(CalciteConnection.class).getRootSchema();


            JsonView jsonView = new JsonView();


            String  sql = "SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'";

            jsonView.sql=sql;

//            connection.unwrap(AvaticaConnection.class).
//            CalciteRootSchema rootSchema = (CalciteRootSchema) connection.unwrap(CalciteConnection.class).getRootSchema();
//            rootSchema.add
//            connection.getr
//            connection.getSchema()
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }



    private void printSqlExplain(Connection connection,String sql){
        CalciteServerStatement st = null;
        try {
            st = connection.createStatement().unwrap(CalciteServerStatement.class);
            CalcitePrepare.Context prepareContext = st.createPrepareContext();
            final FrameworkConfig config = Frameworks.newConfigBuilder()
                    .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                    .defaultSchema(prepareContext.getRootSchema().plus())
                    .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                    .build();

            SqlParser parser = SqlParser.create(sql);
            SqlNode sqlNode = parser.parseQuery();
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(prepareContext.getRootSchema(),
                    Collections.singletonList(""),
                    prepareContext.getTypeFactory(), prepareContext.config());
            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                    catalogReader, prepareContext.getTypeFactory(),
                    SqlValidator.Config.DEFAULT);
            // Validate the initial AST
            SqlNode validNode = validator.validate(sqlNode);

            RelOptPlanner planner = new VolcanoPlanner();
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(prepareContext.getTypeFactory()));

            SqlToRelConverter relConverter = new SqlToRelConverter(
                    NOOP_EXPANDER,
                    validator,
                    catalogReader,
                    cluster,
                    StandardConvertletTable.INSTANCE,
                    SqlToRelConverter.config());

            // Convert the valid AST into a logical plan
            RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

            // Display the logical plan
            System.out.println(
                    RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (SqlParseException e) {
            e.printStackTrace();
        }



    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

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

//    private static Consumer<ResultSet> expect(final String... expected) {
//        return resultSet -> {
//            try {
//                final List<String> lines = new ArrayList<>();
//                CsvTest.collect(lines, resultSet);
//                assertEquals(Arrays.asList(expected), lines);
//            } catch (SQLException e) {
////                throw TestUtil.rethrow(e);TestUtil
//            }
//        };
//    }
//
//    private static void collect(List<String> result, ResultSet resultSet)
//            throws SQLException {
//        final StringBuilder buf = new StringBuilder();
//        while (resultSet.next()) {
//            buf.setLength(0);
//            int n = resultSet.getMetaData().getColumnCount();
//            String sep = "";
//            for (int i = 1; i <= n; i++) {
//                buf.append(sep)
//                        .append(resultSet.getMetaData().getColumnLabel(i))
//                        .append("=")
//                        .append(resultSet.getString(i));
//                sep = "; ";
//            }
//            result.add(Util.toLinux(buf.toString()));
//        }
//    }

}
