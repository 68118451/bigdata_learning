package suishen.elasticsearch.core;


import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregations;
import suishen.elasticsearch.core.query.*;


import java.util.List;

/**
 * Author: Bryant Hang
 * Date: 16/6/14
 * Time: 19:15
 */
public interface ElasticsearchOperations {

    /**
     * @return elasticsearch client
     */
    Client getClient();

    /**
     * Index an object. Will do save or update
     *
     * @param query
     * @return returns the document id
     */
    String index(IndexQuery query);

    /**
     * Bulk operation. Will do save or update
     *
     * @param indices
     * @param deletes
     * @param updates
     */
    void bulkOperation(List<IndexQuery> indices, List<DeleteQuery> deletes, List<UpdateQuery> updates);

    /**
     * Execute the query against elasticsearch and return the first returned object
     *
     * @param query
     * @param clazz
     * @return the first matching object
     */
    <T> T queryForObject(GetQuery query, Class<T> clazz);

    /**
     * Execute the query against elasticsearch and return result as
     *
     * @param query
     * @param clazz
     * @return
     */
    <T> List<T> search(SearchQuery query, Class<T> clazz);

    <T> ListDataResp<T> searchReturnTotalNum(SearchQuery query, Class<T> clazz);

    <T> ListDataWithAggResp<T> searchReturnTotalNumWithAgg(SearchQuery query, Class<T> clazz);

    /**
     * Execute aggregation query common
     * TODO: 聚合计算的结果与请求相关,数据结构随着请求的不同而不同,暂时无法归纳出统一的业务相关格式。解析需要业务方完成
     *
     * @param query
     * @return Aggregations
     */
    Aggregations commonAggregation(SearchQuery query);



}