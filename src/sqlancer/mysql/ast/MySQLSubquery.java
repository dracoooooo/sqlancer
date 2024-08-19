package sqlancer.mysql.ast;

public class MySQLSubquery implements MySQLFromItem{
    private final MySQLSelect select;

    public MySQLSubquery(MySQLSelect select) {
        this.select = select;
    }

    @Override
    public String getName() {
        return "subquery";
    }

    public MySQLSelect getSelect() {
        return select;
    }
}
