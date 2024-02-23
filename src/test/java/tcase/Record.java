package tcase;

import java.io.Serializable;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:50
 * description:
 */
public class Record implements Serializable {
    private static final long serialVersionUID = 7373984872572414699L;
    public Record() {

    }
    String key = "c5a3a38f-59b1-4bea-941e-665880f5e607";
    Student student = new Student();

    @Override
    public boolean equals(Object o) {
        if(o instanceof Record) {
           return key.equals(((Record) o).key) && student.equals(((Record) o).student);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
