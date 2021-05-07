package cc.infocloud.engine.mongo;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class MetaDataSchema extends AbstractSchema {
    final MongoDatabase mongoDatabase;


    public MetaDataSchema(String host,String database,MongoClientOptions options) {
        super();
        try{
            final MongoClient mongoClient = new MongoClient(new ServerAddress(host),options);
            this.mongoDatabase = mongoClient.getDatabase(database);
        }catch (Exception e){
            throw  new RuntimeException(e);
        }
    }

    @Override
    protected Map<String, Table> getTableMap(){
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (String collectionName : mongoDatabase.listCollectionNames()) {
//            builder.put(collectionName, new MongoTable(collectionName));
        }
        return builder.build();
    }


}
