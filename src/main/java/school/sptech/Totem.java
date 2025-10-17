package school.sptech;

public class Totem {
    private Integer idTotem;
    private String numMac;
    private String instalador;
    private Boolean statusTotem;
    private String dataInstalacao;
    private Modelo modelo;

    public Totem() {
    }

    public Totem(Integer idTotem, String numMac, String instalador, Boolean statusTotem,
                 String dataInstalacao, Modelo modelo) {
        this.idTotem = idTotem;
        this.numMac = numMac;
        this.instalador = instalador;
        this.statusTotem = statusTotem;
        this.dataInstalacao = dataInstalacao;
        this.modelo = modelo;
    }

    public Integer getIdTotem() { return idTotem; }
    public void setIdTotem(Integer idTotem) { this.idTotem = idTotem; }

    public String getNumMac() { return numMac; }
    public void setNumMac(String numMac) { this.numMac = numMac; }

    public String getInstalador() { return instalador; }
    public void setInstalador(String instalador) { this.instalador = instalador; }

    public Boolean getStatusTotem() { return statusTotem; }
    public void setStatusTotem(Boolean statusTotem) { this.statusTotem = statusTotem; }

    public String getDataInstalacao() { return dataInstalacao; }
    public void setDataInstalacao(String dataInstalacao) { this.dataInstalacao = dataInstalacao; }

    public Modelo getModelo() { return modelo; }
    public void setModelo(Modelo modelo) { this.modelo = modelo;}
}
