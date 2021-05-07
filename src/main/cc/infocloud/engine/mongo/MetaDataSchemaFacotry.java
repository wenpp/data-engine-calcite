package cc.infocloud.engine.mongo;

import com.mongodb.MongoClientOptions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MetaDataSchemaFacotry implements SchemaFactory {

    public MetaDataSchemaFacotry() {
    }


    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        final String host =  "localhost";
        final String database = "meta";

        final MongoClientOptions.Builder options = MongoClientOptions.builder();

        return new MetaDataSchema(host,database,options.build());
    }
}
