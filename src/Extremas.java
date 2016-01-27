import java.util.TreeMap;

public class Extremas {

    private TreeMap<Float, String> best;
    private TreeMap<Float, String> worst;
    private Integer size = 5;

    public Extremas(TreeMap<Float, String> best, TreeMap<Float, String> worst) {
        this.best = best;
        this.worst = worst;
    }

    public Extremas(Integer size) {
        best = new TreeMap<>();
        worst = new TreeMap<>();
        this.size = size;
    }

    public Extremas() {
    }


    public void addBest(Float f, String string){
        best.put(f, string);
        if ( best.size() >5){
            best.remove(best.firstKey());
        }
    }

    public void addWorst(Float f, String string){
        worst.put(f, string);
        if ( worst.size() >5){
            worst.remove(worst.lastKey());
        }
    }

    public TreeMap<Float, String> getBest() {
        return best;
    }

    public void setBest(TreeMap<Float, String> best) {
        this.best = best;
    }

    public TreeMap<Float, String> getWorst() {
        return worst;
    }

    public void setWorst(TreeMap<Float, String> worst) {
        this.worst = worst;
    }

}