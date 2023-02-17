package com.suishen.elasticsearch.core.query;

import com.suishen.elasticsearch.meta.meta.req.JsonSerializerType;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import com.suishen.libs.data.PageRequest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: Bryant Hang
 * Date: 16/6/15
 * Time: 16:56
 */
public class SearchQuery {
    private List<String> indices = new ArrayList<String>();
    private List<String> types = new ArrayList<String>();
    private SearchType searchType = SearchType.DFS_QUERY_THEN_FETCH;
    private PageRequest pageable = new PageRequest();
    private QueryBuilder query;
    private QueryBuilder filter;
    private List<AbstractAggregationBuilder> aggs;
    private List<SortBuilder> sorts;
    private JsonSerializerType jsonSerializerType;
    private float minScore;
    private List<String> sourceIncludes;
    private List<String> sourceExcludes;

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public List<String> getTypes() {
        return types;
    }

    public void setTypes(List<String> types) {
        this.types = types;
    }

    public SearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(SearchType searchType) {
        this.searchType = searchType;
    }

    public PageRequest getPageable() {
        return pageable;
    }

    public void setPageable(PageRequest pageable) {
        this.pageable = pageable;
    }

    public float getMinScore() {
        return minScore;
    }

    public void setMinScore(float minScore) {
        this.minScore = minScore;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public void setQuery(QueryBuilder query) {
        this.query = query;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    public void setFilter(QueryBuilder filter) {
        this.filter = filter;
    }

    public List<AbstractAggregationBuilder> getAggs() {
        return aggs;
    }

    public void setAggs(List<AbstractAggregationBuilder> aggs) {
        this.aggs = aggs;
    }

    public List<SortBuilder> getSorts() {
        return sorts;
    }

    public void setSorts(List<SortBuilder> sorts) {
        this.sorts = sorts;
    }

    public JsonSerializerType getJsonSerializerType() {
        return jsonSerializerType;
    }

    public void setJsonSerializerType(JsonSerializerType jsonSerializerType) {
        this.jsonSerializerType = jsonSerializerType;
    }

    public List<String> getSourceIncludes() {
        return sourceIncludes;
    }

    public void setSourceIncludes(List<String> sourceIncludes) {
        this.sourceIncludes = sourceIncludes;
    }

    public List<String> getSourceExcludes() {
        return sourceExcludes;
    }

    public void setSourceExcludes(List<String> sourceExcludes) {
        this.sourceExcludes = sourceExcludes;
    }

    public static Builder newBuilder(){
        return new Builder();
    }

    public static class Builder {

        private List<String> indices;
        private List<String> types;
        private SearchType searchType;
        private PageRequest pageable;
        private QueryBuilder query;
        private QueryBuilder filter;
        private List<AbstractAggregationBuilder> aggs;
        private List<SortBuilder> sorts;
        private float minScore;
        private JsonSerializerType jsonSerializerType;
        private List<String> sourceIncludes;
        private List<String> sourceExcludes;

        public Builder() {
            this.indices = new ArrayList<>();
            this.types = new ArrayList<>();
            this.searchType = SearchType.DFS_QUERY_THEN_FETCH;
            this.pageable = new PageRequest();
            this.aggs = new ArrayList<>();
            this.sorts = new ArrayList<>();
            this.jsonSerializerType = JsonSerializerType.FASTJSON;
            this.sourceIncludes = new ArrayList<>();
            this.sourceExcludes = new ArrayList<>();
        }

        public Builder addIndices(String... indices) {
            Collections.addAll(this.indices, indices);
            return this;
        }

        public Builder addTypes(String... types) {
            Collections.addAll(this.types, types);
            return this;
        }

        public Builder setSearchType(SearchType searchType) {
            this.searchType = searchType;
            return this;
        }

        public Builder setPageable(PageRequest pageable) {
            this.pageable = pageable;
            return this;
        }

        public Builder setQuery(QueryBuilder query) {
            this.query = query;
            return this;
        }

        public Builder setFilter(QueryBuilder filter) {
            this.filter = filter;
            return this;

        }

        public Builder addAgg(AbstractAggregationBuilder... aggs){
            Collections.addAll(this.aggs,aggs);
            return this;
        }

        public Builder addSort(SortBuilder... sorts){
            Collections.addAll(this.sorts,sorts);
            return this;
        }

        public Builder setMinScore(float minScore) {
            this.minScore = minScore;
            return this;
        }

        public Builder setSerializerType(JsonSerializerType jsonSerializerType){
            this.jsonSerializerType = jsonSerializerType;
            return this;
        }

        public Builder addSourceExclude(String... excludes){
            Collections.addAll(this.sourceExcludes,excludes);
            return this;
        }

        public Builder addSourceInclude(String... includes){
            Collections.addAll(this.sourceIncludes,includes);
            return this;
        }

        public Builder addSourceExclude(List<String> excludes){
            if(CollectionUtils.isNotEmpty(excludes)){
                this.sourceExcludes.addAll(excludes);
            }
            return this;
        }

        public Builder addSourceInclude(List<String> includes){
            if(CollectionUtils.isNotEmpty(includes)) {
                this.sourceIncludes.addAll(includes);
            }
            return this;
        }

        public SearchQuery build() {
            SearchQuery query = new SearchQuery();
            query.setIndices(this.indices);
            query.setTypes(this.types);
            query.setPageable(this.pageable);
            query.setQuery(this.query);
            query.setFilter(this.filter);
            query.setMinScore(this.minScore);
            query.setSearchType(this.searchType);
            query.setAggs(this.aggs);
            query.setSorts(this.sorts);
            query.setJsonSerializerType(this.jsonSerializerType);
            query.setSourceIncludes(this.sourceIncludes);
            query.setSourceExcludes(this.sourceExcludes);

            return query;
        }
    }
}
