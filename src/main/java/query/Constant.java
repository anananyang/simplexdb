package query;

public class Constant implements Comparable<Constant>{
    private Integer ival = null;
    private String sval = null;

    public Constant(Integer ival) {
        this.ival = ival;
    }

    public Constant(String sval) {
        this.sval = sval;
    }

    public Integer getIval() {
        return ival;
    }

    public String getSval() {
        return sval;
    }

    public int asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    @Override
    public int compareTo(Constant constant) {
        return ival != null
                ? ival.compareTo(constant.getIval())
                : sval.compareTo(constant.getSval());
    }

    @Override
    public boolean equals(Object o) {
        Constant c = (Constant)o;
        return ival != null
                ? ival.equals(c.getIval())
                : sval.equals(c.getSval());
    }

    @Override
    public int hashCode() {
        return ival != null ? ival.hashCode() : sval.hashCode();
    }

    @Override
    public String toString() {
        return ival != null ? ival.toString() : sval;
    }
}
