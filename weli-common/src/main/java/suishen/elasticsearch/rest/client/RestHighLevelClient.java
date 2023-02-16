package suishen.elasticsearch.rest.client;

import suishen.elasticsearch.core.query.DeleteQuery;
import suishen.elasticsearch.core.query.IndexQuery;
import suishen.elasticsearch.core.query.UpdateQuery;
import suishen.elasticsearch.core.query.BulkRequest;
import suishen.elasticsearch.rest.client.config.HttpHostConfig;
import com.google.common.base.Charsets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 09/08/2017
 * Time: 18:27
 */
public class RestHighLevelClient {

    private static final Logger LOG = LoggerFactory.getLogger(RestHighLevelClient.class);
    private final RestClient client;

    public RestHighLevelClient(List<HttpHostConfig> configs) throws Exception {
        if (CollectionUtils.isEmpty(configs)) {
            throw new Exception("elasticsearch high level rest api config empty");
        }
        List<HttpHost> hosts = new ArrayList<>();
        for (HttpHostConfig config : configs) {
            hosts.add(new HttpHost(config.getHost(), config.getPort(), config.getSchema()));
        }
        this.client = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()])).build();
    }

    public boolean index(final IndexQuery indexQuery, TimeValue timeout) {
        try {
            Request req = Request.index(indexQuery, timeout);
            Response response = client.performRequest(req.method, req.endpoint, req.params, req.entity);
            if (response == null) {
                return false;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= HttpStatus.SC_OK && statusCode <= HttpStatus.SC_MULTI_STATUS) {
                return true;
            } else {
                LOG.error(EntityUtils.toString(response.getEntity(), Charsets.UTF_8));
                return false;
            }
        } catch (IOException e) {
            LOG.error("index fail", e);
            return false;
        }
    }


    public boolean update(final UpdateQuery updateQuery, TimeValue timeout) {
        try {
            Request req = Request.update(updateQuery, timeout);
            Response response = client.performRequest(req.method, req.endpoint, req.params, req.entity);
            if (response == null) {
                return false;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= HttpStatus.SC_OK && statusCode <= HttpStatus.SC_MULTI_STATUS) {
                return true;
            } else {
                LOG.error(EntityUtils.toString(response.getEntity(), Charsets.UTF_8));
                return false;
            }
        } catch (IOException e) {
            LOG.error("update fail", e);
            return false;
        }
    }

    public boolean delete(final DeleteQuery deleteQuery, TimeValue timeout) {
        try {
            Request req = Request.delete(deleteQuery, timeout);
            Response response = client.performRequest(req.method, req.endpoint, req.params, req.entity);
            if (response == null) {
                return false;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= HttpStatus.SC_OK && statusCode <= HttpStatus.SC_MULTI_STATUS) {
                return true;
            } else {
                LOG.error(EntityUtils.toString(response.getEntity(), Charsets.UTF_8));
                return false;
            }
        } catch (IOException e) {
            LOG.error("delete fail", e);
            return false;
        }
    }

    public boolean bulk(final BulkRequest bulkRequest, TimeValue timeout) {
        try {
            Request req = Request.bulk(bulkRequest, timeout);
            Response response = client.performRequest(req.method, req.endpoint, req.params, req.entity);
            if (response == null) {
                return false;
            }
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= HttpStatus.SC_OK && statusCode <= HttpStatus.SC_MULTI_STATUS) {
                return true;
            } else {
                LOG.error(EntityUtils.toString(response.getEntity(), Charsets.UTF_8));
                return false;
            }
        } catch (IOException e) {
            LOG.error("bulk fail", e);
            return false;
        }
    }


    public void close() throws IOException {
        client.close();
    }
}
