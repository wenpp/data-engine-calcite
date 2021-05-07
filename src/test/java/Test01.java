//import cc.infocloud.utils.CalciteUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
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
import org.apache.calcite.tools.*;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Test01 {
//    @Test
//    public void printPlan(){
//        String filePath = "/model-view.yaml";
//        Connection connection = null;
//        Statement statement = null;
//        try{
////            connection = CalciteUtil.getConnect(filePath);
//            statement = connection.createStatement();
//            String sql = "select * from csv.TEST01 as t1 left join csv.TEST02 as t2 on t1.NAME1=t2.NAME3";
//
//            ResultSet resultSet = statement.executeQuery(sql);
//            System.out.println("-------------------------  " +
//                    "start sql"
//                    + "  -------------------------  ");
//            String pretty = JSON.toJSONString(CalciteUtil.getData(resultSet),
//                    SerializerFeature.PrettyFormat,
//                    SerializerFeature.WriteMapNullValue,
//                    SerializerFeature.WriteDateUseDateFormat);
//            System.out.println(pretty);
//            System.out.println("-------------------------  " +
//                    "end sql"
//                    + "  -------------------------  ");
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void printSqlInfo(){
//        Queue<RelNode> relNodeQueue = new LinkedList<RelNode>();
//        String filePath = "/model-view.yaml";
//        Connection connection = null;
//        Statement statement = null;
//        String sql = "select * from csv.TEST01 as t1 left join csv.TEST02 as t2 on t1.NAME1=t2.NAME3";
//
////        connection = CalciteUtil.getConnect(filePath);
//
//        try {
//            CalciteServerStatement st = connection.createStatement().unwrap(CalciteServerStatement.class);
//            CalcitePrepare.Context prepareContext = st.createPrepareContext();
//            final FrameworkConfig config = Frameworks.newConfigBuilder()
//                    .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
//                    .defaultSchema(prepareContext.getRootSchema().plus())
//                    .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
//                    .build();
//
//            //Parse the query into an AST
//            SqlParser parser = SqlParser.create(sql);
//            SqlNode sqlNode = parser.parseQuery();
//            CalciteCatalogReader catalogReader = new CalciteCatalogReader(prepareContext.getRootSchema(),
//                    Collections.singletonList(""),
//                    prepareContext.getTypeFactory(), prepareContext.config());
//            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
//                    catalogReader, prepareContext.getTypeFactory(),
//                    SqlValidator.Config.DEFAULT);
//            // Validate the initial AST
//            SqlNode validNode = validator.validate(sqlNode);
//
//            RelOptPlanner planner = new VolcanoPlanner();
//            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
//            RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(prepareContext.getTypeFactory()));
//
//            SqlToRelConverter relConverter = new SqlToRelConverter(
//                    NOOP_EXPANDER,
//                    validator,
//                    catalogReader,
//                    cluster,
//                    StandardConvertletTable.INSTANCE,
//                    SqlToRelConverter.config());
//
//            // Convert the valid AST into a logical plan
//            RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
//
//            // Display the logical plan
//            System.out.println(
//                    RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
//
//
//            // Initialize optimizer/planner with the necessary rules
//            RelOptPlanner secondPlanner = cluster.getPlanner();
//            secondPlanner.addRule(CoreRules.FILTER_INTO_JOIN);
//            secondPlanner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
//            secondPlanner.addRule(Bindables.BINDABLE_FILTER_RULE);
//            secondPlanner.addRule(Bindables.BINDABLE_JOIN_RULE);
//            secondPlanner.addRule(Bindables.BINDABLE_PROJECT_RULE);
//            secondPlanner.addRule(Bindables.BINDABLE_SORT_RULE);
//
//            // Define the type of the output plan (in this case we want a physical plan in
//            // BindableConvention)
//            logPlan = secondPlanner.changeTraits(logPlan,
//                    cluster.traitSet().replace(BindableConvention.INSTANCE));
//            secondPlanner.setRoot(logPlan);
//
//            // Start the optimization process to obtain the most efficient physical plan based on the
//            // provided rule set.
//            BindableRel phyPlan = (BindableRel) secondPlanner.findBestExp();
//
//            // Display the physical plan
//            System.out.println(
//                    RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.NON_COST_ATTRIBUTES));
//
//            // Run the executable plan using a context simply providing access to the schema
//            for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(prepareContext.getRootSchema()))) {
//                System.out.println(Arrays.toString(row));
//            }
//
//        } catch (SQLException | SqlParseException throwables) {
//            throwables.printStackTrace();
//        }
//
//
//
//
////        try {
////            RelRoot root = genRelRoot(connection,sql);
////        } catch (SQLException e) {
////            e.printStackTrace();
////        }finally {
////            try {
////                connection.close();
////            } catch (SQLException e) {
////                e.printStackTrace();
////            }
////        }
//
//    }
//
//    private static void printInfo(RelNode rel){
//        Queue<RelNode> relNodeQueue = new LinkedList<RelNode>();
//        relNodeQueue.offer(rel);
//        /**
//         * RelNode类型：
//         * TableScan（获取表信息，列信息）
//         * 一元节点：
//         * LogicalJoin
//         * Sort
//         * GROUP BY
//         * ......
//         *
//         * 二元信息：
//         * LogicalJoin
//         * Union
//         * ......
//         *
//         */
//        int joinCount = 0;
//        int aggregateCount = 0;
//        //层次遍历树并获取信息
//        while (relNodeQueue.size() != 0) {
//            int inputNum = relNodeQueue.size();
//            for (int i = 0; i < inputNum; i++) {
//                RelNode tem = relNodeQueue.poll();
//                for (RelNode r : tem.getInputs()) {
//                    relNodeQueue.offer(r);
//                }
//                if (tem.getRelTypeName().contains("Join")) {
//                    joinCount += 1;
//                }
//                if (tem.getRelTypeName().contains("Aggregate")) {
//                    aggregateCount += 1;
//                }
//                //print table info
//                if (tem.getTable() != null) {
//                    RelOptTable rtable = tem.getTable();
//                    System.out.println("------------------ table " + rtable.getQualifiedName() + " scan info: ------------------");
//                    System.out.println("row name and type : " + rtable.getRowType());
//                    System.out.println("distribution info : " + rtable.getDistribution());  //由 RelDistribution 的类型决定
//                    System.out.println("columns strategies : " + rtable.getColumnStrategies());
//                    System.out.println("------------------end table " + rtable.getQualifiedName() + " ------------------");
//                }
////                RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
//            }
//        }
//        //print sql info
//        System.out.println("Join num is : " + joinCount);
//        System.out.println("Aggregate num is : " + joinCount);
//
////        System.out.println("After------------------");
//    }
//    private static RelRoot genRelRoot(Connection connection, String sql) throws SQLException {
//        //从 conn 中获取相关的环境和配置，生成对应配置
//        CalciteServerStatement st = connection.createStatement().unwrap(CalciteServerStatement.class);
//        CalcitePrepare.Context prepareContext = st.createPrepareContext();
//        final FrameworkConfig config = Frameworks.newConfigBuilder()
//                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
//                .defaultSchema(prepareContext.getRootSchema().plus())
//                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
//                .build();
//
//        Planner planner = Frameworks.getPlanner(config);
//        RelRoot root = null;
//        try {
//            SqlNode parse = planner.parse(sql);
//            SqlNode validate = planner.validate(parse);
//            root = planner.rel(validate);
//            RelNode rel = root.rel;
//            System.out.println(RelOptUtil.dumpPlan("[Logical plan]",rel,SqlExplainFormat.TEXT,SqlExplainLevel.EXPPLAN_ATTRIBUTES));
////            BindableRel phyPlan = (BindableRel) planner.findBestExp();
//
//
//            //physiac plan
//            final VolcanoPlanner planner2 = new VolcanoPlanner();
//            planner2.addRelTraitDef(ConventionTraitDef.INSTANCE);
//
//            final SqlToRelConverter.Config configs =
//                    SqlToRelConverter.config().withTrimUnusedFields(true);
//
//            JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
//
//            CalciteCatalogReader catalogReader = new CalciteCatalogReader(prepareContext.getRootSchema(),
//                    Collections.singletonList(""),
//                    typeFactory, prepareContext.config());
//
//            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),catalogReader,typeFactory,SqlValidator.Config.DEFAULT);
//
//            RelOptCluster cluster = RelOptCluster.create(planner2,new RexBuilder(typeFactory));
//
//            SqlToRelConverter converters =  new SqlToRelConverter(NOOP_EXPANDER, validator, catalogReader,cluster ,StandardConvertletTable.INSTANCE, configs);
//
//            RelOptPlanner planner3 = cluster.getPlanner();
//
//
//            planner3.setRoot(planner3.changeTraits(rel,cluster.traitSet().replace(BindableConvention.INSTANCE)));
//            BindableRel phyPlan = (BindableRel) planner3.findBestExp();
//            System.out.println(
//                    RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
//                            SqlExplainLevel.NON_COST_ATTRIBUTES));
//
//
//
//
////            RelOptPlanner planner1 =
//        } catch (SqlParseException | ValidationException | RelConversionException e) {
//            e.printStackTrace();
//        }
//        return root;
//    }
//
//    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
//            , viewPath) -> null;
//
//    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
//        RelOptPlanner planner = new VolcanoPlanner();
//        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
//        return RelOptCluster.create(planner, new RexBuilder(factory));
//    }
//
//    private static final class SchemaOnlyDataContext implements DataContext {
//        private final SchemaPlus schema;
//
//        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
//            this.schema = calciteSchema.plus();
//        }
//
//        @Override public SchemaPlus getRootSchema() {
//            return schema;
//        }
//
//        @Override public JavaTypeFactory getTypeFactory() {
//            return new JavaTypeFactoryImpl();
//        }
//
//        @Override public QueryProvider getQueryProvider() {
//            return null;
//        }
//
//        @Override public Object get(final String name) {
//            return null;
//        }
//    }
}
