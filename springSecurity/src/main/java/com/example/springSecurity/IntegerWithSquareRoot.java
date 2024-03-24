package com.example.springSecurity;

public class IntegerWithSquareRoot {

    private Integer originalValue;
    private Double squareRootValue;
    public IntegerWithSquareRoot(int i) {
        this.originalValue =i;
        this.squareRootValue = Math.sqrt(originalValue);

    }
}
