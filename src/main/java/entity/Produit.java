package entity;

import lombok.Data;
import org.json.simple.JSONObject;

@Data
public class Produit {
    private JSONObject before;
    private JSONObject after;
    private JSONObject source;
    private String op;
    private Long ts_ms;
    private String transaction;
}
