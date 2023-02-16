package suishen.elasticsearch.meta.meta.query;

/**
 * Author: Alvin Li
 * Date: 7/12/16
 * Time: 17:52
 */
public enum NumType {
    INT("integer"),
    LONG("long"),
    DOUBLE("double"),
    FLOAT("float");
    private String name;

    NumType(String name) {
        this.name = name;
    }
}
