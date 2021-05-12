import cc.infocloud.csv.scanable.CsvScannableTable;
import cc.infocloud.csv.scanable.ViewStmt;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.csv.CsvSchema;
import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.adapter.csv.CsvTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

public class MetaDataMock {
    private static final List<Object[]> ProductData = Arrays.asList(
            new Object[]{"data001","SP0001","OBJ001","Iphon8 256G","Product00001",6000,"Online","Mobile Phone","Apple"},
            new Object[]{"data002","SP0001","OBJ001","IphonX 256G","Product00002",10000,"Online","Mobile Phone","Apple"},
            new Object[]{"data003","SP0001","OBJ001","IphonXR 256G","Product00003",8000,"Online","Mobile Phone","Apple"},
            new Object[]{"data004","SP0001","OBJ001","HUAWEI P30","Product00004",5000,"Online","Mobile Phone","HUAWEI"}
    );

    private static final List<Object[]> CustomerData = Arrays.asList(
            new Object[]{"data005","SP0001","OBJ002","Cheng Yan","Customer0001","Yan","Cheng","cy","chengyan","Valid"},
            new Object[]{"data006","SP0001","OBJ002","Ling Jun","Customer0002","Jun","Ling","lj","lingjun","Valid"},
            new Object[]{"data007","SP0001","OBJ002","Tommy Valdels","Customer0003","Tommy","Valdels","tom","Tommy","Valid"},
            new Object[]{"data008","SP0001","OBJ002","Dorothy Franklin","Customer0004","Dorothy","Franklin","doro","Dorothy","Valid"}
    );

    private static final List<Object[]> OrderData = Arrays.asList(
            new Object[]{"data009","SP0001","OBJ003","Order","Order0001","data005","Completed","2019-08-01T04:00:00.000+0000"},
            new Object[]{"data010","SP0001","OBJ003","Order","Order0002","data006","Completed","2019-03-09T11:00:00.000+0000"},
            new Object[]{"data011","SP0001","OBJ003","Order","Order0003","data008","Completed","2019-03-08T23:30:00.000+0000"}
    );

    private static final List<Object[]> OrderItemData = Arrays.asList(
            new Object[]{"data012","SP0001","OBJ004","Order Item","data009","data001","5888","2","InProduction"},
            new Object[]{"data013","SP0001","OBJ004","Order Item","data009","data002","9888","3","Canceled"},
            new Object[]{"data014","SP0001","OBJ004","Order Item","data010","data003","7888","1","InTransfer"},
            new Object[]{"data014","SP0001","OBJ004","Order Item","data011","data003","7888","1","InTransfer"}
    );
    @Test
    public void testDefineView(){
        Connection connection = null;
        Statement statement = null;
//        String model = "model-view-data";
        String model = "model-view-bydefine";

        Properties info = new Properties();
        info.put("model", jsonPath(model));

        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);

            statement = connection.createStatement();

            //1-define view manully
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            List<String> path = Lists.newArrayList("META");
            final List<String> viewPath1 = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
            calciteConnection.getRootSchema().add("PRODUCT",ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'",path,viewPath1,true));

            final List<String> viewPath2 = ImmutableList.<String>builder().addAll(path).add("CUSTOMER").build();
            calciteConnection.getRootSchema().add("CUSTOMER",ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT GUID,ORGID,NAME,VALUE1 as CustomerNo,VALUE6 as CustomerStatus FROM DATA WHERE OBJID = 'object002'",path,viewPath2,true));

            final List<String> viewPath3 = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
            calciteConnection.getRootSchema().add("ORDERS",ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT GUID,ORGID,VALUE1 as OrderNo,VALUE2 as CustomerNo,VALUE3 as OrderStatus FROM DATA WHERE OBJID = 'object003'",path,viewPath3,true));

            final List<String> viewPath4 = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
            calciteConnection.getRootSchema().add("ORDERITEM",ViewTable.viewMacro(calciteConnection.getRootSchema(),"SELECT GUID,ORGID,VALUE1 as OrderNo,VALUE2 as ProductNo,VALUE3 as ItemPrice,VALUE4 as ItemQuanity,VALUE5 as OrderItemStatus FROM DATA WHERE OBJID = 'object004'",path,viewPath4,true));

            //2-define sql
//            String sql = "SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'";
            //view sql
//            String sql = "SELECT * FROM ORDERITEM";
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
    @Test
    public void testViewByDefineParse(){
        File directoryFile = new File("/Users/wenpeng/Documents/java_workspace/data-engine-calcite/target/test-classes/metadata/DATA.csv");
        final Source baseSource = Sources.of(directoryFile);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

        //init table
        CsvScannableTable metaDataTable = new CsvScannableTable(baseSource,null);

        schemaPlus.add("DATA",metaDataTable);

        //inint view
        List<String> path = Lists.newArrayList("DATA");
        final List<String> viewPath = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
        schemaPlus.add("PRODUCT",ViewTable.viewMacro(schemaPlus,"SELECT GUID,ORGID,NAME,VALUE1,VALUE2 FROM DATA WHERE OBJID = 'object001'",path,viewPath,true));


        String veiwSql = "SELECT * FROM PRODUCT";
        SqlParser parser = SqlParser.create(veiwSql);

        // Parse the query into an AST
        try{
            SqlNode sqlNode = parser.parseQuery();

            Properties props = new Properties();
            props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(schemaPlus),
                    Collections.singletonList(""),
                    typeFactory, config);

            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                    catalogReader, typeFactory,
                    SqlValidator.Config.DEFAULT);

            // Validate the initial AST
            SqlNode validNode = validator.validate(sqlNode);

            // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
//            RelOptCluster cluster = newCluster(typeFactory);typeFactory

            VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(catalogReader.getConfig()));
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

//            Frameworks.newConfigBuilder().defaultSchema(schemaPlus).sqlToRelConverterConfig(SqlToRelConverter.config()).parserConfig()

            PlannerImpl plannerImpl = new PlannerImpl(Frameworks
                    .newConfigBuilder()
                    .defaultSchema(schemaPlus)
                    .sqlToRelConverterConfig(SqlToRelConverter.config())
                    .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
                    .build());

            RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
            RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);


            SqlToRelConverter relConverter = new SqlToRelConverter(
                    plannerImpl,
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

            // Initialize optimizer/planner with the necessary rules
            RelOptPlanner planner2 = cluster.getPlanner();
            planner2.addRule(CoreRules.FILTER_INTO_JOIN);
//            planner2.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
//            planner2.addRule(Bindables.BINDABLE_FILTER_RULE);
//            planner2.addRule(Bindables.BINDABLE_JOIN_RULE);
//            planner2.addRule(Bindables.BINDABLE_PROJECT_RULE);
//            planner2.addRule(Bindables.BINDABLE_SORT_RULE);

            // Define the type of the output plan (in this case we want a physical plan in
            // BindableConvention)
            logPlan = planner.changeTraits(logPlan,
                    cluster.traitSet().replace(BindableConvention.INSTANCE));
            planner.setRoot(logPlan);
            // Start the optimization process to obtain the most efficient physical plan based on the
            // provided rule set.
            BindableRel phyPlan = (BindableRel) planner.findBestExp();

            // Display the physical plan
            System.out.println(
                    RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.NON_COST_ATTRIBUTES));

            // Run the executable plan using a context simply providing access to the schema
//            Hook.QUERY_PLAN.addThread((Consumer<Object>) s-> Log.info("Execute sql:"+s));
            for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(CalciteSchema.from(schemaPlus),typeFactory))) {
                System.out.println(Arrays.toString(row));
            }
        }catch (Exception e){
            System.out.println(e.getCause());
        }

    }


    @Test
    public void testView(){
        File directoryFile = new File("/Users/wenpeng/Documents/java_workspace/data-engine-calcite/target/test-classes/metadata/DATA.csv");
        final Source baseSource = Sources.of(directoryFile);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        //init schema

        CalciteSchema schema = CalciteSchema.createRootSchema(true,true,"META");

        //init table
        CsvScannableTable metaDataTable = new CsvScannableTable(baseSource,null);

        schema.add("DATA",metaDataTable);

        //init view
        List<String> path = Util.first(null, schema.getPath().get(0));
        final List<String> viewPath = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
        schema.plus().add("PRODUCT", ViewTable.viewMacro(schema.plus(),"SELECT GUID,ORGID,NAME,VALUE1,VALUE2 FROM DATA WHERE OBJID = 'object001'",path,viewPath,true));

        String veiwSql = "SELECT * FROM PRODUCT";
//        String veiwSql = "SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'";
        SqlParser parser = SqlParser.create(veiwSql);

        // Parse the query into an AST
        try {
            SqlNode sqlNode = parser.parseQuery();

            Properties props = new Properties();
            props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                    Collections.singletonList(""),
                    typeFactory, config);

            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                    catalogReader, typeFactory,
                    SqlValidator.Config.DEFAULT);

            // Validate the initial AST
            SqlNode validNode = validator.validate(sqlNode);

            // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
            RelOptCluster cluster = newCluster(typeFactory);

//            ViewExpanders.simpleContext(cluster).expandView(null,veiwSql,schema.getPath().get(0),viewPath);


            SqlToRelConverter relConverter = new SqlToRelConverter(
                    new ViewStmt(sqlNode,schema,typeFactory,cluster),
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

            // Initialize optimizer/planner with the necessary rules
            RelOptPlanner planner = cluster.getPlanner();
            planner.addRule(CoreRules.FILTER_INTO_JOIN);
            planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
            planner.addRule(Bindables.BINDABLE_FILTER_RULE);
            planner.addRule(Bindables.BINDABLE_JOIN_RULE);
            planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
            planner.addRule(Bindables.BINDABLE_SORT_RULE);

            // Define the type of the output plan (in this case we want a physical plan in
            // BindableConvention)
            logPlan = planner.changeTraits(logPlan,
                    cluster.traitSet().replace(BindableConvention.INSTANCE));
            planner.setRoot(logPlan);
            // Start the optimization process to obtain the most efficient physical plan based on the
            // provided rule set.
            BindableRel phyPlan = (BindableRel) planner.findBestExp();

            // Display the physical plan
            System.out.println(
                    RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.NON_COST_ATTRIBUTES));

            // Run the executable plan using a context simply providing access to the schema
//            Hook.QUERY_PLAN.addThread((Consumer<Object>) s-> Log.info("Execute sql:"+s));
            for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema,typeFactory))) {
                System.out.println(Arrays.toString(row));
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testTable(){
        File directoryFile = new File("/Users/wenpeng/Documents/java_workspace/data-engine-calcite/target/test-classes/metadata/DATA.csv");
        final Source baseSource = Sources.of(directoryFile);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        //init schema
        CalciteSchema schema = CalciteSchema.createRootSchema(true,true,"META");

        //init table
        CsvScannableTable metaDataTable = new CsvScannableTable(baseSource,null);

        schema.add("DATA",metaDataTable);

        //init view
        List<String> path = Util.first(null, schema.getPath().get(0));
        final List<String> viewPath = ImmutableList.<String>builder().addAll(path).add("PRODUCT").build();
        schema.plus().add("PRODUCT", ViewTable.viewMacro(schema.plus(),"SELECT GUID,ORGID,NAME,VALUE1 as ProductNo,VALUE2 as ProductPrice  FROM DATA WHERE OBJID = 'object001'",path,viewPath,true));

//        //view2
//        List<String> path2 = Util.first(null, schema.path("META"));
//        final List<String> viewPath2 = ImmutableList.<String>builder().addAll(path).add("CUSTOMER").build();
//        schema.plus().add("CUSTOMER", ViewTable.viewMacro(schema.plus(),"SELECT GUID,ORGID,NAME,VALUE1 as CustomerNo,VALUE6 as CustomerStatus FROM DATA WHERE OBJID = 'object002'",path,viewPath,false));
//
//        //view3
//        List<String> path3 = Util.first(null, schema.path("META"));
//        final List<String> viewPath3 = ImmutableList.<String>builder().addAll(path).add("ORDERS").build();
//        schema.plus().add("ORDERS", ViewTable.viewMacro(schema.plus(),"SELECT GUID,ORGID,VALUE1 as OrderNo,VALUE2 as CustomerNo,VALUE3 as OrderStatus FROM DATA WHERE OBJID = 'object003'",path,viewPath,false));


//        SqlParser parser = SqlParser.create("select * from DATA");
        SqlParser parser = SqlParser.create("select t2.NAME,t2.VALUE1,t3.VALUE1,t1.NAME as ProductName,t1.VALUE2,t4.VALUE3 from Data t1,Data t2,Data t3,Data t4 where t4.VALUE2 = t1.VALUE1 and t3.VALUE1 = t4.VALUE1 and t2.VALUE1 = t3.VALUE2 and t2.NAME='Cheng Yan' and t4.OBJID = 'object004' and t3.OBJID = 'object003' and t2.OBJID = 'object002' and t1.OBJID = 'object001'");
//        SqlParser parser = SqlParser.create("SELECT * FROM PRODUCT");

        // Parse the query into an AST
        try {
            SqlNode sqlNode = parser.parseQuery();

            Properties props = new Properties();
            props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                    Collections.singletonList(""),
                    typeFactory, config);

            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                    catalogReader, typeFactory,
                    SqlValidator.Config.DEFAULT);

            // Validate the initial AST
            SqlNode validNode = validator.validate(sqlNode);

            // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
            RelOptCluster cluster = newCluster(typeFactory);

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

            // Initialize optimizer/planner with the necessary rules
            RelOptPlanner planner = cluster.getPlanner();
            planner.addRule(CoreRules.FILTER_INTO_JOIN);
            planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
            planner.addRule(Bindables.BINDABLE_FILTER_RULE);
            planner.addRule(Bindables.BINDABLE_JOIN_RULE);
            planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
            planner.addRule(Bindables.BINDABLE_SORT_RULE);

            // Define the type of the output plan (in this case we want a physical plan in
            // BindableConvention)
            logPlan = planner.changeTraits(logPlan,
                    cluster.traitSet().replace(BindableConvention.INSTANCE));
            planner.setRoot(logPlan);
            // Start the optimization process to obtain the most efficient physical plan based on the
            // provided rule set.
            BindableRel phyPlan = (BindableRel) planner.findBestExp();

            // Display the physical plan
            System.out.println(
                    RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                            SqlExplainLevel.NON_COST_ATTRIBUTES));

            // Run the executable plan using a context simply providing access to the schema
//            Hook.QUERY_PLAN.addThread((Consumer<Object>) s-> Log.info("Execute sql:"+s));
            for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema,typeFactory))) {
                System.out.println(Arrays.toString(row));
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void example() throws Exception{
        //init schema
        CalciteSchema schema = CalciteSchema.createRootSchema(true);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        //define Product table(Name,ProductNo,ProductPrice,ProductStatus,Category,Brand)
        RelDataTypeFactory.Builder productType = new RelDataTypeFactory.Builder(typeFactory);
        productType.add("GUID", SqlTypeName.VARCHAR);
        productType.add("OrgID", SqlTypeName.VARCHAR);
        productType.add("ObjID", SqlTypeName.VARCHAR);
        productType.add("Name", SqlTypeName.VARCHAR);
        productType.add("ProductNo",SqlTypeName.VARCHAR);
//        productType.add("ProductPrice",SqlTypeName.VARCHAR);
        productType.add("ProductPrice",SqlTypeName.INTEGER);
        productType.add("ProductStatus",SqlTypeName.VARCHAR);
        productType.add("Category",SqlTypeName.VARCHAR);
        productType.add("Brand",SqlTypeName.VARCHAR);

        ListTable productTable = new ListTable(productType.build(),ProductData);

        schema.add("product", productTable);

        //sql
        SqlParser parser = SqlParser.create("select Name,ProductNo,ProductPrice,ProductStatus,Category,Brand from product where ProductPrice > 9000");

        // Parse the query into an AST
        SqlNode sqlNode = parser.parseQuery();

        // Configure and instantiate validator
        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                Collections.singletonList(""),
                typeFactory, config);

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        // Validate the initial AST
        SqlNode validNode = validator.validate(sqlNode);

        // Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
        RelOptCluster cluster = newCluster(typeFactory);
        SqlToRelConverter relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
//        SqlToRelConverter.Config config1 = SqlToRelConverter.config().withTrimUnusedFields(true);
//        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter()


        // Convert the valid AST into a logical plan
        RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;

        // Initialize optimizer/planner with the necessary rules
        RelOptPlanner planner = cluster.getPlanner();
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(Bindables.BINDABLE_FILTER_RULE);
        planner.addRule(Bindables.BINDABLE_JOIN_RULE);
        planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
        planner.addRule(Bindables.BINDABLE_SORT_RULE);

        // Display the logical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.EXPPLAN_ATTRIBUTES));

        // Define the type of the output plan (in this case we want a physical plan in
        // BindableConvention)
        logPlan = planner.changeTraits(logPlan,
                cluster.traitSet().replace(BindableConvention.INSTANCE));
        planner.setRoot(logPlan);
        // Start the optimization process to obtain the most efficient physical plan based on the
        // provided rule set.
        BindableRel phyPlan = (BindableRel) planner.findBestExp();

        // Display the physical plan
        System.out.println(
                RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
                        SqlExplainLevel.NON_COST_ATTRIBUTES));

        // Run the executable plan using a context simply providing access to the schema
//        for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema))) {
//            System.out.println(Arrays.toString(row));
//        }

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

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

    private static final RelOptTable.ViewExpander VIEW_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> {

        return null;
    };

//    private static RelOptTable.ViewExpander initExpandView(SqlNode sqlNode){
//        return new ViewStmt(sqlNode);
//    }

//    private static final RelOptTable.ViewExpander EXPAND_VIEW=(rowType, queryString, schemaPath, viewPath) ->

//    private static RelRoot expandView(RelDataType rowType, String queryString,
//                                      List<String> schemaPath, List<String> viewPath){
//
//    }

    private class ViewExplaner implements RelOptTable.ViewExpander{
        RelOptCluster cluster;

        public ViewExplaner(RelOptCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
            return ViewExpanders.simpleContext(cluster).expandView(rowType,queryString,schemaPath,viewPath);
        }
    }


    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;
        private RelDataTypeFactory typeFactory;

        SchemaOnlyDataContext(CalciteSchema calciteSchema,RelDataTypeFactory typeFactory) {
            this.schema = calciteSchema.plus();
            this.typeFactory = typeFactory;

        }

        @Override public SchemaPlus getRootSchema() {
            return schema;
        }

//        @Override public JavaTypeFactory getTypeFactory() {
//            return new JavaTypeFactoryImpl();
//        }
        @Override public JavaTypeFactory getTypeFactory() {
            return (JavaTypeFactory) typeFactory;
        }

        @Override public QueryProvider getQueryProvider() {
            return null;
        }

        @Override public Object get(final String name) {
            return null;
        }
    }
}
