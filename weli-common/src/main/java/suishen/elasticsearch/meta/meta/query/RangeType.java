package suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 11:43
 */
public enum RangeType {
    GTE("greater or equal"),
    GT("greater"),
    LTE("less or equal"),
    LT("less")
    ;

    RangeType(String type) {
        this.type = type;
    }

    private String type;

    public String getType() {
        return type;
    }
}
