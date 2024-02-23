package tcase;

import lombok.Setter;

import java.io.Serializable;

/**
 * Author: Stan sai
 * Date: 2024/2/22 21:51
 * description:
 */
public class Student implements Serializable {
    private static final long serialVersionUID = 7373984872572414699L;
    public Student() {

    }
    public Student(long id) {
        this.id = id;
    }
    String name = "adhyqhuehuqehuiiu是缺u且飞机";
    @Setter
    long id = 9L;
    String phone = "76278147817893891893189";
    String address = "阿护齿画画后期会期间急哦全集哦放假哦IE去切记哦飞机uhiqehuhui今请回话呼唤iu请诶ii切记";
    String[] hobbies = new String[] {"ajjajk", "ajcfhiuqiuiueqiufheqo", "hjqehjhjeqjh"};

    @Override
    public boolean equals(Object o) {
        if(o instanceof Student) {
            return ((Student) o).id == this.id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return String.valueOf(id).hashCode();
    }
}