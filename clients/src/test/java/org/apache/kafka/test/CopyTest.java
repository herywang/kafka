package org.apache.kafka.test;


public class CopyTest {

    static class Person{
        int age;
        String name;
        public Person(){
            this.age = 5;
            this.name = "dddd";
        }
    }

    public static void main(String[] args) {
        Person p = new Person();
        Person p2 = new Person();
        p2.name = "p2";
        Person b = p;
        p = p2;
//        p.name = "aaaaa";
        System.out.println(b.age+"-"+b.name);
    }

}
