package school.sptech;

public class Parametro {
    private Integer idParametro;
    private Integer limiteMin;
    private Integer limiteMax;
    private TipoParametro tipoParametro;
    private Modelo modelo;

    public Parametro() {
    }

    public Parametro(Integer idParametro, Integer limiteMin, Integer limiteMax, TipoParametro tipoParametro, Modelo modelo) {
        this.idParametro = idParametro;
        this.limiteMin = limiteMin;
        this.limiteMax = limiteMax;
        this.tipoParametro = tipoParametro;
        this.modelo = modelo;
    }

    public Integer getIdParametro() {
        return idParametro;
    }

    public void setIdParametro(Integer idParametro) {
        this.idParametro = idParametro;
    }

    public Integer getLimiteMin() {
        return limiteMin;
    }

    public void setLimiteMin(Integer limiteMin) {
        this.limiteMin = limiteMin;
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
