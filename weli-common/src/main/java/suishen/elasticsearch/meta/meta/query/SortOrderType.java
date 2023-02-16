package suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 12/10/16
 * Time: 15:40
 */
public enum SortOrderType {
    ASC("asc"),
    DESC("desc")
    ;
    private String order;

    SortOrderType(String order) {
        this.order = order;
    }

    public String getOrder() {
        return order;
    }
}
