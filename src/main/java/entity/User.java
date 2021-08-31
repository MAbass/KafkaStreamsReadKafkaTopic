package entity;

import lombok.Data;
import org.json.simple.JSONObject;

import java.io.Serializable;

@Data
public class User implements Serializable {
    protected JSONObject before;
    protected JSONObject after;
    protected JSONObject source;
    protected String op;
    protected Long ts_ms;
    protected String transaction;

}
