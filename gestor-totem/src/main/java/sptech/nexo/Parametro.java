package sptech.nexo;

public class Parametro {
    //Atributos
    private Integer idParametro;
    private Integer limiteMax;
    private TipoParametro tipoParametro;
    private Modelo modelo;

    //Construtor Cheio e Vazio
    public Parametro(Integer idParametro, Integer limiteMax, TipoParametro tipoParametro, Modelo modelo) {
        this.idParametro = idParametro;
        this.limiteMax = limiteMax;
        this.tipoParametro = tipoParametro;
        this.modelo = modelo;
    }
    public Parametro() {}

    //Getters e Setters
    public Integer getIdParametro() {
        return idParametro;
    }
    public void setIdParametro(Integer idParametro) {
        this.idParametro = idParametro;
    }
    public Integer getLimiteMax() {
        return limiteMax;
    }
    public void setLimiteMax(Integer limiteMax) {
        this.limiteMax = limiteMax;
    }
    public TipoParametro getTipoParametro() {
        return tipoParametro;
    }
    public void setTipoParametro(TipoParametro tipoParametro) {
        this.tipoParametro = tipoParametro;
    }
    public Modelo getModelo() {
        return modelo;
    }
    public void setModelo(Modelo modelo) {
        this.modelo = modelo;
    }
}
