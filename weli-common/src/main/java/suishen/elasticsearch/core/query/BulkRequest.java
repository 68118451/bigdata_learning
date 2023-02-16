package suishen.elasticsearch.core.query;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Alvin Li
 * Date: 11/08/2017
 * Time: 14:44
 */
public class BulkRequest {
    private static final Logger LOG = LoggerFactory.getLogger(BulkRequest.class);

    final private List<BulkOperation> requests = new ArrayList<>();

    public List<BulkOperation> getRequests() {
        return requests;
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(requests);
    }

    public BulkRequest add(IndexQuery request) {
        if (request == null) {
            LOG.warn("index request is null");
            return this;
        }
        if (!validateWithObject(request)) {
            return this;
        }

        requests.add(request);
        return this;
    }

    public BulkRequest add(UpdateQuery request) {
        if (request == null) {
            LOG.warn("update request is null");
            return this;
        }
        if (!validateWithObject(request)) {
            return this;
        }

        requests.add(request);
        return this;
    }

    public BulkRequest add(DeleteQuery request) {
        if (request == null) {
            LOG.warn("delete request is null");
            return this;
        }
        if (!validateWithoutObject(request)) {
            return this;
        }

        requests.add(request);
        return this;
    }

    private boolean validateWithoutObject(BulkOperation request) {
        if (StringUtils.isBlank(request.getIndexName())) {
            LOG.warn("index is null");
            throw new IllegalArgumentException("index can't be null");
        }
        if (StringUtils.isBlank(request.getType())) {
            LOG.warn("type is null");
            throw new IllegalArgumentException("type can't be null");
        }

        if (StringUtils.isBlank(request.getId())) {
            LOG.warn("id is null");
            throw new IllegalArgumentException("id can't be null");
        }
        return true;
    }

    private boolean validateWithObject(BulkOperation request) {
        if (!validateWithoutObject(request)) {
            return false;
        }
        if (request.getObject() == null && StringUtils.isBlank(request.getSource())) {
            LOG.info("index object or source is null");
            throw new IllegalArgumentException("index object or source can't be null");
        }

        return true;
    }
}
