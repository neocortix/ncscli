package com.example;
 
public class helloFrame {
    public static void main(String[] args) {
        int frameNum = 0;
        if (args.length > 0)
            frameNum = Integer.parseInt(args[0]);
        System.out.println("Hello, world! From frameNum = " + frameNum);
    }
};
