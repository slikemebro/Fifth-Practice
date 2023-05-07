package com.ua.glebkorobov.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Objects;

public class Product {

    private String name;

    private String type;

    private int quantity;

    private String address;


    public Product(String name, String type, int quantity, String address) {
        this.name = name;
        this.type = type;
        this.quantity = quantity;
        this.address = address;
    }

    public Product() {
    }

    @Size(min = 5, max = 15, message = "Size should be between 5 and 15")
    @NotBlank(message = "Name could be null or blank")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Size(min = 5, max = 15, message = "Size should be between 5 and 15")
    @NotBlank(message = "Type could be null or blank")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @NotNull(message = "Quantity couldn't be null")
    @Min(value = 1, message = "Quantity couldn't be less than zero")
    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Size(min = 5, max = 15, message = "Size should be between 5 and 15")
    @NotBlank(message = "Address could be null or blank")
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Product)) return false;
        Product product = (Product) o;
        return Objects.equals(getName(), product.getName()) && Objects.equals(getType(), product.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getType());
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
