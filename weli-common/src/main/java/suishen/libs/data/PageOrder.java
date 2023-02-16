package suishen.libs.data;

/**
 * Created by Bryant.Hang on 2017/11/2.
 */
public enum PageOrder {
    ASC("asc"), DESC("desc");

    private String val;

    PageOrder(String val) {
        this.val = val;
    }

    public String val() {
        return val;
    }
}
