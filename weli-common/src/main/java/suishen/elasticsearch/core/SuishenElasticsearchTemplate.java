package suishen.elasticsearch.core;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.elasticsearch.index.VersionType;
import suishen.elasticsearch.ElasticsearchException;

import suishen.elasticsearch.core.query.*;
import suishen.elasticsearch.meta.meta.req.JsonSerializerType;
import suishen.elasticsearch.util.EsConstant;
import suishen.elasticsearch.util.JsonUtils;
import suishen.elasticsearch.annotations.Document;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import suishen.libs.meta.SuishenConstant;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.index.VersionType.EXTERNAL;

/**
 * Author: Bryant Hang
 * Date: 16/6/13
 * Time: 17:36
 */
public class SuishenElasticsearchTemplate implements EsConstant, SuishenConstant, ElasticsearchOperations, ElasticsearchAdminOperations {
    private static final Logger LOG = LoggerFactory.getLogger(SuishenElasticsearchTemplate.class);
    private String clusterNodes;

    private String clusterName;

    private TransportClient client;

    private String searchTimeout;

    public SuishenElasticsearchTemplate(String clusterNodes, String clusterName) throws UnknownHostException {
        this.clusterNodes = Objects.requireNonNull(clusterNodes);
        this.clusterName = Objects.requireNonNull(clusterName);

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", this.clusterName)
                .put("client.transport.sniff", true).build();

        this.client = TransportClient.builder().settings(settings).build();

        for (String node : StringUtils.split(this.clusterNodes, SuishenConstant.COMMA)) {
            String hostName = StringUtils.substringBeforeLast(node, SuishenConstant.COLON);
            String port = StringUtils.substringAfterLast(node, SuishenConstant.COLON);

            this.client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), NumberUtils.toInt(port)));
        }
        this.client.connectedNodes();
    }

    public void setSearchTimeout(String searchTimeout) {
        this.searchTimeout = searchTimeout;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public String index(IndexQuery query) {
        return prepareIndex(query).get().getId();
    }

    /**
     * bulk操作
     *
     * @param indices 全量更新或者索引
     * @param deletes 删除
     * @param updates 局部更新
     */
    @Override
    public void bulkOperation(List<IndexQuery> indices, List<DeleteQuery> deletes, List<UpdateQuery> updates) {
        if ((indices == null || indices.size() == 0) && (deletes == null || deletes.size() == 0) && (updates == null || updates.size() == 0))
            return;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        if (indices != null)
            for (IndexQuery query : indices) {
                bulkRequest.add(prepareIndex(query));
            }
        if (deletes != null)
            for (DeleteQuery delete : deletes) {
                bulkRequest.add(prepareDelete(delete));
            }
        if (updates != null)
            for (UpdateQuery update : updates) {
                bulkRequest.add(prepareUpdate(update));
            }
        if (bulkRequest.numberOfActions() == 0) {
            return;
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            Map<String, String> failedDocuments = new HashMap<String, String>();
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (item.isFailed())
                    failedDocuments.put(item.getId(), item.getFailureMessage());
            }
            throw new ElasticsearchException(
                    "Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
                            + failedDocuments + "]", failedDocuments
            );
        }
    }

    @Override
    public <T> T queryForObject(GetQuery query, Class<T> clazz) {
        GetRequestBuilder getRequestBuilder = client
                .prepareGet(query.getIndexName(), query.getIndexType(), query.getId());

        String[] sourceIncludes = new String[]{};
        String[] sourceExcludes = new String[]{};
        if (CollectionUtils.isNotEmpty(query.getSourceIncludes())) {
            sourceIncludes = query.getSourceIncludes().toArray(new String[query.getSourceIncludes().size()]);
        }
        if (CollectionUtils.isNotEmpty(query.getSourceExcludes())) {
            sourceExcludes = query.getSourceExcludes().toArray(new String[query.getSourceExcludes().size()]);
        }
        getRequestBuilder.setFetchSource(sourceIncludes, sourceExcludes);

        GetResponse response = getRequestBuilder.execute()
                .actionGet();
        if (query.getJsonSerializerType() == JsonSerializerType.FASTJSON) {
            return JSON.parseObject(response.getSourceAsString(), clazz);
        } else {
            return JsonUtils.json2Object(response.getSourceAsString(), clazz);
        }

    }

    @Override
    public <T> List<T> search(SearchQuery query, Class<T> clazz) {
        SearchRequestBuilder builder = prepareSearch(query);

        if (query.getFilter() != null) {
            builder.setPostFilter(query.getFilter());
        }
        if (query.getSorts() != null) {
            for (SortBuilder sb : query.getSorts()) {
                builder.addSort(sb);
            }
        }

        builder.setQuery(query.getQuery());
        LOG.debug(builder.toString());
        SearchResponse response = getSearchResponse(builder.execute());

        List<T> result = Lists.newArrayListWithCapacity(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits()) {
            if (hit != null) {
                if (query.getJsonSerializerType() == JsonSerializerType.FASTJSON) {
                    result.add(JSON.parseObject(hit.getSourceAsString(), clazz));
                } else {
                    result.add(JsonUtils.json2Object(hit.getSourceAsString(), clazz));
                }
            }
        }

        return result;
    }

    @Override
    public <T> ListDataResp<T> searchReturnTotalNum(SearchQuery query, Class<T> clazz) {
        SearchRequestBuilder builder = prepareSearch(query);

        if (query.getFilter() != null) {
            builder.setPostFilter(query.getFilter());
        }
        if (query.getSorts() != null) {
            for (SortBuilder sb : query.getSorts()) {
                builder.addSort(sb);
            }
        }
        builder.setQuery(query.getQuery());

        LOG.debug(builder.toString());
        SearchResponse response = getSearchResponse(builder.execute());
        List<T> result = Lists.newArrayListWithCapacity(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits()) {
            if (hit != null) {
                if (query.getJsonSerializerType() == JsonSerializerType.FASTJSON) {
                    result.add(JSON.parseObject(hit.getSourceAsString(), clazz));
                } else {
                    result.add(JsonUtils.json2Object(hit.getSourceAsString(), clazz));
                }
            }
        }
        ListDataResp<T> listResp = new ListDataResp();
        listResp.setData(result);
        listResp.setTotal(response.getHits().getTotalHits());
        return listResp;
    }

    @Override
    public <T> ListDataWithAggResp<T> searchReturnTotalNumWithAgg(SearchQuery query, Class<T> clazz) {
        SearchRequestBuilder builder = prepareSearchWithAgg(query);

        if (query.getFilter() != null) {
            builder.setPostFilter(query.getFilter());
        }
        if (query.getSorts() != null) {
            for (SortBuilder sb : query.getSorts()) {
                builder.addSort(sb);
            }
        }
        builder.setQuery(query.getQuery());

        LOG.debug(builder.toString());
        SearchResponse response = getSearchResponse(builder.execute());
        List<T> result = Lists.newArrayListWithCapacity(response.getHits().getHits().length);
        for (SearchHit hit : response.getHits()) {
            if (hit != null) {
                if (query.getJsonSerializerType() == JsonSerializerType.FASTJSON) {
                    result.add(JSON.parseObject(hit.getSourceAsString(), clazz));
                } else {
                    result.add(JsonUtils.json2Object(hit.getSourceAsString(), clazz));
                }
            }
        }
        ListDataWithAggResp<T> listResp = new ListDataWithAggResp<>();
        listResp.setData(result);
        listResp.setTotal(response.getHits().getTotalHits());
        listResp.setAggs(response.getAggregations());

        return listResp;
    }

    @Override
    public Aggregations commonAggregation(SearchQuery query) {
        SearchRequestBuilder builder = prepareAgg(query);
        builder.setQuery(query.getQuery());
        LOG.debug(builder.toString());
        SearchResponse response = getSearchResponse(builder.execute());

        return response.getAggregations();
    }


    private SearchResponse getSearchResponse(ListenableActionFuture<SearchResponse> response) {
        try {
            return searchTimeout == null ? response.actionGet() : response.actionGet(searchTimeout);
        } catch (QueryParsingException e) {
            LOG.debug("query parsing error:", e);
            return new SearchResponse(InternalSearchResponse.empty(), "", 0, 0, 0, new ShardSearchFailure[]{});
        } catch (SearchPhaseExecutionException e) {
            LOG.debug("search phase execution error:", e);
            return new SearchResponse(InternalSearchResponse.empty(), "", 0, 0, 0, new ShardSearchFailure[]{});
        } catch (Exception e) {
            LOG.error("search error", e);
            return new SearchResponse(InternalSearchResponse.empty(), "", 0, 0, 0, new ShardSearchFailure[]{});
        }

    }

    private IndexRequestBuilder prepareIndex(IndexQuery query) {
        if (query.getId() == null) {
            throw new IllegalArgumentException("id can't be null");
        }
        String indexName = getIndexNameFromDocument(query.getObject());
        String type = getTypeFromDocument(query.getObject());
        if ("".equals(indexName))
            indexName = query.getIndexName();
        if ("".equals(type))
            type = query.getType();

        IndexRequestBuilder indexRequestBuilder;

        if (query.getObject() != null && isDocument(query.getObject().getClass())) {
            JsonSerializerType jsonSerializerType = getJsonSerializerTypeFromDocument(query.getObject());
            if (jsonSerializerType == JsonSerializerType.FASTJSON) {
                indexRequestBuilder = client.prepareIndex(indexName, type, query.getId()).setSource(JSON.toJSONString(query.getObject(), SerializerFeature.WriteMapNullValue));
            } else {
                indexRequestBuilder = client.prepareIndex(indexName, type, query.getId()).setSource(JsonUtils.object2Json(query.getObject()));
            }
        } else if (query.getSource() != null) {
            indexRequestBuilder = client.prepareIndex(indexName, type, query.getId()).setSource(query.getSource());
        } else {
            throw new ElasticsearchException("object or source is null, failed to index the document [id: " + query.getId() + "]");
        }

        if (query.getVersion() != null) {
            indexRequestBuilder.setVersion(query.getVersion());
            indexRequestBuilder.setVersionType(VersionType.EXTERNAL);
        }

        if (query.getParentId() != null) {
            indexRequestBuilder.setParent(query.getParentId());
        }

        return indexRequestBuilder;
    }

    private UpdateRequestBuilder prepareUpdate(UpdateQuery query) {
        if (query.getId() == null) {
            throw new IllegalArgumentException("id can't be null");
        }
        String indexName = getIndexNameFromDocument(query.getObject());
        String type = getTypeFromDocument(query.getObject());
        if ("".equals(indexName))
            indexName = query.getIndexName();
        if ("".equals(type))
            type = query.getType();

        UpdateRequestBuilder updateRequestBuilder;

        if (query.getObject() != null && isDocument(query.getObject().getClass())) {
            JsonSerializerType jsonSerializerType = getJsonSerializerTypeFromDocument(query.getObject());
            if (jsonSerializerType == JsonSerializerType.FASTJSON) {
                updateRequestBuilder = client.prepareUpdate(indexName, type, query.getId()).setDoc(JSON.toJSONString(query.getObject(), SerializerFeature.WriteMapNullValue)).setDocAsUpsert(query.isUpsert());
            } else {
                updateRequestBuilder = client.prepareUpdate(indexName, type, query.getId()).setDoc(JsonUtils.object2Json(query.getObject())).setDocAsUpsert(query.isUpsert());
            }
        } else if (query.getSource() != null) {
            updateRequestBuilder = client.prepareUpdate(indexName, type, query.getId()).setDoc(query.getSource()).setDocAsUpsert(query.isUpsert());
        } else {
            throw new ElasticsearchException("object or source is null, failed to index the document [id: " + query.getId() + "]");
        }

        if (query.getVersion() != null) {
            updateRequestBuilder.setVersion(query.getVersion());
            updateRequestBuilder.setVersionType(VersionType.EXTERNAL);
        }

        if (query.getParentId() != null) {
            updateRequestBuilder.setParent(query.getParentId());
        }

        return updateRequestBuilder;
    }

    private DeleteRequestBuilder prepareDelete(DeleteQuery delete) {
        if (delete.getId() == null) {
            throw new IllegalArgumentException("id can't be null");
        }
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(delete.getIndexName(), delete.getType(), delete.getId());

        if (delete.getVersion() != null) {
            deleteRequestBuilder.setVersion(delete.getVersion());
            deleteRequestBuilder.setVersionType(VersionType.EXTERNAL);
        }
        if (delete.getParentId() != null) {
            deleteRequestBuilder.setParent(delete.getParentId());
        }
        return deleteRequestBuilder;
    }

    private SearchRequestBuilder prepareSearch(SearchQuery query) {

        int startRecord = 0;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(query.getIndices().toArray(new String[]{}))
                .setTypes(query.getTypes().toArray(new String[]{})).setSearchType(query.getSearchType());

        if (query.getPageable() != null) {
            startRecord = query.getPageable().getPageNumber() * query.getPageable().getPageSize();
            searchRequestBuilder.setSize(query.getPageable().getPageSize());
        }
        searchRequestBuilder.setFrom(startRecord);

        if (query.getMinScore() > 0) {
            searchRequestBuilder.setMinScore(query.getMinScore());
        }

        String[] sourceIncludes = new String[]{};
        String[] sourceExcludes = new String[]{};
        if (CollectionUtils.isNotEmpty(query.getSourceIncludes())) {
            sourceIncludes = query.getSourceIncludes().toArray(new String[query.getSourceIncludes().size()]);
        }
        if (CollectionUtils.isNotEmpty(query.getSourceExcludes())) {
            sourceExcludes = query.getSourceExcludes().toArray(new String[query.getSourceExcludes().size()]);
        }
        searchRequestBuilder.setFetchSource(sourceIncludes, sourceExcludes);

        searchRequestBuilder.setExplain(false);

        return searchRequestBuilder;
    }

    /**
     * 获得包含
     *
     * @param query
     * @return
     */
    private SearchRequestBuilder prepareSearchWithAgg(SearchQuery query) {
        int startRecord = 0;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(query.getIndices().toArray(new String[]{}))
                .setTypes(query.getTypes().toArray(new String[]{})).setSearchType(query.getSearchType());

        if (query.getPageable() != null) {
            startRecord = query.getPageable().getPageNumber() * query.getPageable().getPageSize();
            searchRequestBuilder.setSize(query.getPageable().getPageSize());
        }
        searchRequestBuilder.setFrom(startRecord);

        if (query.getMinScore() > 0) {
            searchRequestBuilder.setMinScore(query.getMinScore());
        }

        String[] sourceIncludes = new String[]{};
        String[] sourceExcludes = new String[]{};
        if (CollectionUtils.isNotEmpty(query.getSourceIncludes())) {
            sourceIncludes = query.getSourceIncludes().toArray(new String[query.getSourceIncludes().size()]);
        }
        if (CollectionUtils.isNotEmpty(query.getSourceExcludes())) {
            sourceExcludes = query.getSourceExcludes().toArray(new String[query.getSourceExcludes().size()]);
        }
        searchRequestBuilder.setFetchSource(sourceIncludes, sourceExcludes);

        searchRequestBuilder.setExplain(false);

        for (AbstractAggregationBuilder agg : query.getAggs()) {
            searchRequestBuilder.addAggregation(agg);
        }

        return searchRequestBuilder;
    }

    /**
     * 初始化进行agg操作的默认参数
     *
     * @param query
     * @return
     */
    private SearchRequestBuilder prepareAgg(SearchQuery query) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(query.getIndices().toArray(new String[]{}))
                .setTypes(query.getTypes().toArray(new String[]{}))
                .setSearchType(query.getSearchType())
                .setExplain(false);
        for (AbstractAggregationBuilder agg : query.getAggs()) {
            searchRequestBuilder.addAggregation(agg);
        }
        if (query.getPageable() != null) {
            searchRequestBuilder.setSize(query.getPageable().getPageSize());
        }
        if (query.getMinScore() > 0) {
            searchRequestBuilder.setMinScore(query.getMinScore());
        }

        return searchRequestBuilder;
    }

    private boolean isDocument(Class clazz) {
        return clazz.isAnnotationPresent(Document.class);
    }

    private JsonSerializerType getJsonSerializerTypeFromDocument(Object obj) {
        if (obj == null) {
            return JsonSerializerType.FASTJSON;
        }
        if (!obj.getClass().isAnnotationPresent(Document.class)) {
            return JsonSerializerType.FASTJSON;
        } else {
            Document anno = obj.getClass().getAnnotation(Document.class);
            return anno.jsonSerialiizer();
        }
    }


    private String getIndexNameFromDocument(Object obj) {
        if (obj == null) {
            return "";
        }
        if (obj.getClass().isAnnotationPresent(Document.class)) {
            Document anno = obj.getClass().getAnnotation(Document.class);
            return anno.indexName();
        } else {
            return "";
        }
    }

    private String getTypeFromDocument(Object obj) {
        if (obj == null) {
            return "";
        }
        if (obj.getClass().isAnnotationPresent(Document.class)) {
            Document anno = obj.getClass().getAnnotation(Document.class);
            return anno.type();
        } else {
            return "";
        }
    }


    @Override
    public boolean modifyAlias(AliasIndexHolder[] add, AliasIndexHolder[] remove) {
        IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
        for (AliasIndexHolder ai : add) {
            builder.addAlias(ai.getIndex(), ai.getAlias());
        }
        for (AliasIndexHolder ai : remove) {
            builder.removeAlias(ai.getIndex(), ai.getAlias());
        }
        IndicesAliasesResponse resp = builder.execute().actionGet();
        return resp.isAcknowledged();
    }

    @Override
    public List<String> getIndicesByAliasName(String alias) {
        List<String> result = new ArrayList<>();
        if (alias == null || "".equals(alias)) {
            return result;
        }
        ImmutableOpenMap<String, List<AliasMetaData>> map = client.admin().indices().prepareGetAliases(alias).get().getAliases();
        UnmodifiableIterator<String> it = map.keysIt();
        while (it.hasNext()) {
            result.add(it.next());
        }
        return result;
    }

    @Override
    public List<String> getIndicesByPattern(String pattern) {
        String[] indices = client.admin().indices().prepareGetIndex().execute().actionGet().indices();
        if (pattern == null || "".equals(pattern)) {
            return Arrays.asList(indices);
        }
        List<String> indexList = new ArrayList<>();
        Pattern p = Pattern.compile(pattern);
        for (String s : indices) {
            if (p.matcher(s).matches()) {
                indexList.add(s);
            }
        }
        return indexList;

    }

    @Override
    public boolean isIndexExist(String index) {
        return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    }

    @Override
    public boolean isAliasExist(String alias) {
        return client.admin().indices().prepareAliasesExist(alias).execute().actionGet().isExists();
    }

    @Override
    public boolean createTemplate(String templateName, String template) {
        return client.admin().indices().preparePutTemplate(templateName).setSource(template).execute().actionGet().isAcknowledged();
    }

    @Override
    public boolean deleteTemplate(String templateName) {
        return client.admin().indices().prepareDeleteTemplate(templateName).execute().actionGet().isAcknowledged();
    }

    @Override
    public List<String> getAllAlias() {
        ImmutableOpenMap<String, List<AliasMetaData>> map = client.admin().indices().prepareGetAliases().execute().actionGet().getAliases();
        UnmodifiableIterator<String> it = map.keysIt();
        Set<String> aliasSet = new HashSet<>();
        while (it.hasNext()) {
            for (AliasMetaData amd : map.get(it.next())) {
                aliasSet.add(amd.getAlias());
            }
        }
        return new ArrayList<>(aliasSet);
    }

    @Override
    public boolean createIndex(String index, String alias) {
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(index);
        if (alias != null && !"".equals(alias)) {
            builder.addAlias(new Alias(alias));
        }
        return builder.execute().actionGet().isAcknowledged();
    }

    @Override
    public boolean deleteIndex(String... index) {
        Set<String> indices = new HashSet<>();
        for (String i : index) {
            if (isIndexExist(i)) {
                indices.add(i);
            }
        }
        if (indices.size() == 0) {
            return true;
        }
        return client.admin().indices().prepareDelete(indices.toArray(new String[indices.size()])).execute().actionGet().isAcknowledged();
    }

    @Override
    public void optimizeIndex(String... name) {
        client.admin().indices().prepareForceMerge(name).execute().actionGet();
    }
}
