package school.sptech;

public class Parametro {
    private String modelo;
    private String parametro;
    private Double limite;

    public Parametro() {
    }

    public Parametro(String modelo, String parametro, Double limite) {
        this.modelo = modelo;
        this.parametro = parametro;
        this.limite = limite;
    }

    @Override
    public String toString() {
        return "Parametro{" +
                "modelo='" + modelo + '\'' +
                ", parametro='" + parametro + '\'' +
                ", limite=" + limite +
                '}';
    }

    public String getModelo() {
        return modelo;
    }

    public void setModelo(String modelo) {
        this.modelo = modelo;
    }

    public String getParametro() {
        return parametro;
    }

    public void setParametro(String parametro) {
        this.parametro = parametro;
    }

    public Double getLimite() {
        return limite;
    }

    public void setLimite(Double limite) {
        this.limite = limite;
    }
}