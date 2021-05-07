package cc.infocloud.engine.mongo;

import com.mongodb.client.MongoCursor;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;
import org.bson.Document;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoEnumerator implements Enumerator<Object> {
    private final Iterator<Document> cursor;
    private final Function1<Document, Object> getter;
    private Object current;

    public MongoEnumerator(Iterator<Document> cursor, Function1<Document, Object> getter) {
        this.cursor = cursor;
        this.getter = getter;
    }

    @Override
    public Object current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try{
            if(cursor.hasNext()){
                Document map = cursor.next();
                current = getter.apply(map);
                return true;
            }else{
                current = null;
                return false;
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if(cursor instanceof MongoCursor){
            ((MongoCursor<Document>) cursor).close();
        }
    }

    static Function1<Document, Map> mapGetter() {
        return a0 -> (Map) a0;
    }

    static Function1<Document, Object> singletonGetter(final String fieldName,
                                                       final Class fieldClass) {
        return a0 -> convert(a0.get(fieldName), fieldClass);
    }

    static Function1<Document, Object[]> listGetter(
            final List<Map.Entry<String, Class>> fields) {
        return a0 -> {
            Object[] objects = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                final Map.Entry<String, Class> field = fields.get(i);
                final String name = field.getKey();
                objects[i] = convert(a0.get(name), field.getValue());
            }
            return objects;
        };
    }

    static Function1<Document, Object> getter(
            List<Map.Entry<String, Class>> fields) {
        //noinspection unchecked
        return fields == null
                ? (Function1) mapGetter()
                : fields.size() == 1
                ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue())
                : (Function1) listGetter(fields);
    }

    private static Object convert(Object o, Class clazz) {
        if (o == null) {
            return null;
        }
        Primitive primitive = Primitive.of(clazz);
        if (primitive != null) {
            clazz = primitive.boxClass;
        } else {
            primitive = Primitive.ofBox(clazz);
        }
        if (clazz.isInstance(o)) {
            return o;
        }
        if (o instanceof Date && primitive != null) {
            o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
        }
        if (o instanceof Number && primitive != null) {
            return primitive.number((Number) o);
        }
        return o;
    }

}
