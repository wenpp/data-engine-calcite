
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.model.JsonView;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class EndToEndExampleBindable {
    private static final List<Object[]> BOOK_DATA = Arrays.asList(
            new Object[]{1, "Les Miserables", 1862, 0},
            new Object[]{2, "The Hunchback of Notre-Dame", 1829, 0},
            new Object[]{3, "The Last Day of a Condemned Man", 1829, 0},
            new Object[]{4, "The three Musketeers", 1844, 1},
            new Object[]{5, "The Count of Monte Cristo", 1884, 1}
    );

    private static final List<Object[]> AUTHOR_DATA = Arrays.asList(
            new Object[]{0, "Victor", "Hugo"},
            new Object[]{1, "Alexandre", "Dumas"}
    );

    @Test
    public void example() throws Exception {
        // Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();


        //1. Create the root schema describing the data model
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        // 2.1 Define type for authors table
        RelDataTypeFactory.Builder authorType = new RelDataTypeFactory.Builder(typeFactory);
        authorType.add("id", SqlTypeName.INTEGER);
        authorType.add("fname", SqlTypeName.VARCHAR);
        authorType.add("lname", SqlTypeName.VARCHAR);

        // 2.2 Define type for books table
        RelDataTypeFactory.Builder bookType = new RelDataTypeFactory.Builder(typeFactory);
        bookType.add("id", SqlTypeName.INTEGER);
        bookType.add("title", SqlTypeName.VARCHAR);
        bookType.add("year", SqlTypeName.INTEGER);
        bookType.add("author", SqlTypeName.INTEGER);

        // 3.1 Initialize authors table with data
        ListTable authorsTable = new ListTable(authorType.build(), AUTHOR_DATA);

        // 3.2 Initialize books table with data
        ListTable booksTable = new ListTable(bookType.build(), BOOK_DATA);

        // 4.1 Add authors table to the schema
        schema.add("author", authorsTable);

        // 4.2 Add books table to the schema
        schema.add("book", booksTable);

        // Create an SQL parser
        SqlParser parser = SqlParser.create(
                "SELECT b.id, b.title, b.\"year\", a.fname || ' ' || a.lname \n"
                        + "FROM Book b\n"
                        + "LEFT OUTER JOIN Author a ON b.author=a.id\n"
                        + "WHERE b.\"year\" > 1830\n"
                        + "ORDER BY b.id\n"
                        + "LIMIT 5");
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
//        Hook.QUERY_PLAN.addThread((Consumer<Object>) s-> Log.info("Execute sql:"+s));
        for (Object[] row : phyPlan.bind(new SchemaOnlyDataContext(schema))) {
            System.out.println(Arrays.toString(row));
        }
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

    private static class CustomerView extends JsonView{


    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath
            , viewPath) -> null;

    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override public QueryProvider getQueryProvider() {
            return null;
        }

        @Override public Object get(final String name) {
            return null;
        }
    }


}
