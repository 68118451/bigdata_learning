package suishen.elasticsearch.core;

/**
 * Author: Alvin Li
 * Date: 7/7/16
 * Time: 16:41
 */
public class AliasIndexHolder {
    private String alias;

    private String index;

    public AliasIndexHolder(String alias, String index) {
        this.alias = alias;
        this.index = index;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}
