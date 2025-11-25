package school.sptech;

public class Totem {
    private Integer idTotem;
    private String numMac;
    private String status;
    private Modelo modelo;
    private Integer fkEndereco;

    public Totem() {
    }

    public Totem(Integer idTotem, String numMac, String status, Modelo modelo, Integer fkEndereco) {
        this.idTotem = idTotem;
        this.numMac = numMac;
        this.status = status;
        this.modelo = modelo;
        this.fkEndereco = fkEndereco;
    }

    public Integer getIdTotem() {
        return idTotem;
    }

    public void setIdTotem(Integer idTotem) {
        this.idTotem = idTotem;
    }

    public String getNumMac() {
        return numMac;
    }

    public void setNumMac(String numMac) {
        this.numMac = numMac;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Modelo getModelo() {
        return modelo;
    }

    public void setModelo(Modelo modelo) {
        this.modelo = modelo;
    }

    public Integer getFkEndereco() {
        return fkEndereco;
    }

    public void setFkEndereco(Integer fkEndereco) {
        this.fkEndereco = fkEndereco;
    }
}
