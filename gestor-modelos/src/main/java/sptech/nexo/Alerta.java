package sptech.nexo;

public class Alerta {
    private String modelo;
    private String componente;
    private String grau;

    //Construtor Cheio e Vazio
    public Alerta(String modelo, String componente, String grau) {
        this.modelo = modelo;
        this.componente = componente;
        this.grau = grau;
    }
    public Alerta() {}

    //Getters e Setters
    public String getModelo() {return modelo;}
    public void setModelo(String modelo) {this.modelo = modelo;}
    public String getComponente() {return componente;}
    public void setComponente(String componente) {this.componente = componente;}
    public String getGrau() {return grau;}
    public void setGrau(String grau) {this.grau = grau;}

    //toString()
    @Override
    public String toString() {
        return "Alerta{" +
                "modelo='" + modelo + '\'' +
                ", componente='" + componente + '\'' +
                ", grau='" + grau + '\'' +
                '}';
    }
}
