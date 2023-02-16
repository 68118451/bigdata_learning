package suishen.elasticsearch.core.query;

/**
 * Author: Alvin Li
 * Date: 11/08/2017
 * Time: 14:25
 */
public interface BulkOperation {
    String getId();

    Object getObject();

    String getIndexName();

    String getType();

    boolean isUpsert();

    BulkOperationType opType();

    String getSource();
}
