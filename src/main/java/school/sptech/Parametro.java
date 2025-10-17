package school.sptech;

public class Parametro {
    private Integer idParametro;
    private Integer limite;
    private TipoParametro tipoParametro;
    private Modelo modelo;

    public Parametro() {
    }

    public Parametro(Integer idParametro, Integer limite, TipoParametro tipoParametro, Modelo modelo) {
        this.idParametro = idParametro;
        this.limite = limite;
        this.tipoParametro = tipoParametro;
        this.modelo = modelo;
    }

    public Integer getIdParametro() { return idParametro; }
    public void setIdParametro(Integer idParametro) { this.idParametro = idParametro; }

    public Integer getLimite() { return limite; }
    public void setLimite(Integer limite) { this.limite = limite; }

    public TipoParametro getTipoParametro() { return tipoParametro; }
    public void setTipoParametro(TipoParametro tipoParametro) { this.tipoParametro = tipoParametro; }

    public Modelo getModelo() { return modelo; }
    public void setModelo(Modelo modelo) { this.modelo = modelo; }
}
