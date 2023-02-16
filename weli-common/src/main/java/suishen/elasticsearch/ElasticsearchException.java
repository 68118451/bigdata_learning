package suishen.elasticsearch;

import java.util.Map;

/**
 * Author: Bryant Hang
 * Date: 16/6/14
 * Time: 19:19
 */
public class ElasticsearchException extends RuntimeException {

    private Map<String, String> failedDocuments;

    public ElasticsearchException(String message) {
        super(message);
    }

    public ElasticsearchException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticsearchException(String message, Throwable cause, Map<String, String> failedDocuments) {
        super(message, cause);
        this.failedDocuments = failedDocuments;
    }

    public ElasticsearchException(String message, Map<String, String> failedDocuments) {
        super(message);
        this.failedDocuments = failedDocuments;
    }

    public Map<String, String> getFailedDocuments() {
        return failedDocuments;
    }
}

