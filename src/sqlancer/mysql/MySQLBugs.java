package sqlancer.mysql;

// do not make the fields final to avoid warnings
public final class MySQLBugs {

    // https://bugs.mysql.com/bug.php?id=99127 0.9 > t0.c0 malfunctions when c0 is
    // an INT UNSIGNED
    public static boolean bug99127 = false;

    // https://bugs.mysql.com/99182 BETWEEN malfunctions for DECIMAL and TEXT
    public static boolean bug99181 = false;

    // https://bugs.mysql.com/bug.php?id=99183
    public static boolean bug99183 = false;

    // https://bugs.mysql.com/bug.php?id=95894
    public static boolean bug95894 = false;

    // https://bugs.mysql.com/bug.php?id=99135
    public static boolean bug99135 = false;

    // https://bugs.mysql.com/bug.php?id=111471
    public static boolean bug111471 = false;

    // https://bugs.mysql.com/bug.php?id=112242
    public static boolean bug112242 = false;

    // https://bugs.mysql.com/bug.php?id=112243
    public static boolean bug112243 = false;

    // https://bugs.mysql.com/bug.php?id=112264
    public static boolean bug112264 = false;

    // https://bugs.mysql.com/bug.php?id=114533
    public static boolean bug114533 = false;

    // https://bugs.mysql.com/bug.php?id=114534
    public static boolean bug114534 = false;

    private MySQLBugs() {
    }

}
