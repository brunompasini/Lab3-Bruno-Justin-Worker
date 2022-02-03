package emse;

public class Sale {
    private String product;
    private String country;
    private int price;

    public String getProduct() {
        return product;
    }

    public String getCountry() {
        return country;
    }

    public int getPrice() {
        return price;
    }

    public Sale(String product, String country, int price) {
        this.product = product;
        this.country = country;
        this.price = price;
    }	
}
