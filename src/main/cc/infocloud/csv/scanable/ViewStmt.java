package cc.infocloud.csv.scanable;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
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

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ViewStmt implements RelOptTable.ViewExpander {
    private int expansionDepth;
    private  SqlNode sqlNode;
    private CalciteSchema schema;
    private RelDataTypeFactory typeFactory;
    private RelOptCluster cluster;

    public ViewStmt(SqlNode sqlNode, CalciteSchema schema, RelDataTypeFactory typeFactory,RelOptCluster cluster) {
        this.sqlNode = sqlNode;
        this.schema = schema;
        this.typeFactory = typeFactory;
        this.cluster = cluster;

    }

    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        expansionDepth++;

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
                Collections.singletonList(""),
                typeFactory, config);

        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT);

        final SqlToRelConverter.Config configs =
                SqlToRelConverter.config().withTrimUnusedFields(true);


        SqlToRelConverter sqlToRelConverter = getSqlToRelConverter(validator,catalogReader,configs);

        RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, true);

        --expansionDepth;
        return root;
    }


    protected SqlToRelConverter getSqlToRelConverter(
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            SqlToRelConverter.Config config) {
//        RelOptCluster cluster = newCluster(typeFactory);
        return new SqlToRelConverter(this, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE, config);
    }

//    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
//        RelOptPlanner planner = new VolcanoPlanner();
//        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
//        return RelOptCluster.create(planner, new RexBuilder(factory));
//    }
}
