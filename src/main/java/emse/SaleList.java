package emse;

import java.util.ArrayList;
import java.util.List;

public class SaleList {
    private List<Sale> sales;
    private List<String> countries;
    private List<String> products;

    public SaleList (){
        sales = new ArrayList<Sale>();
        countries = new ArrayList<String>();
        products = new ArrayList<String>();
    }

    public List<Sale> getTransactions() {
        return sales;
    }

    public List<String> getCountries() {
        return countries;
    }

    public List<String> getProducts() {
        return products;
    }

    public void addSale (Sale sale){
        String product=sale.getProduct();
        String country=sale.getCountry();

        sales.add(sale);

        if (!products.contains(product)){
            products.add(product);
        }
        if (!countries.contains(country)){
            countries.add(country);
        }
    }

    public int nBSalesByCountryByProduct(String country,String product){
        int nbOfSales=0;
        for (Sale sale : sales){
            if (sale.getCountry().equals(country) && sale.getProduct().equals(product))nbOfSales++;
        }
        return nbOfSales;
    }

    public int salesByCountryByProduct(String country,String product){
        int sales_nb=0;
        for (Sale sale : sales){
            if (sale.getCountry().equals(country) && sale.getProduct().equals(product))sales_nb+=sale.getPrice();
        }
        return sales_nb;
    }

    public int averageSoldPerCountryPerProduct( String country){
        int totalSold=0;
        for (Sale sale : sales){
            if (sale.getCountry().equals(country) ) {
                totalSold=totalSold+sale.getPrice();
            }
        }
        return totalSold/products.size();
    }	
}
