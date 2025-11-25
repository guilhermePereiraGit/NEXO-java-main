package sptech.nexo;

public class Totem {
    //Atributos
    private Integer idTotem;
    private String numMac;
    private String nomeModelo;
    private String nomeRegiao;
    private String siglaRegiao;

    //Construtor Cheio e Vazio
    public Totem(Integer idTotem, String numMac, String nomeModelo, String nomeRegiao, String siglaRegiao) {
        this.idTotem = idTotem;
        this.numMac = numMac;
        this.nomeModelo = nomeModelo;
        this.nomeRegiao = nomeRegiao;
        this.siglaRegiao = siglaRegiao;
    }

    public Totem() {}

    //Getters e Setters
    public Integer getIdTotem() {return idTotem;}
    public void setIdTotem(Integer idTotem) {this.idTotem = idTotem;}
    public String getNumMac() {return numMac;}
    public void setNumMac(String numMac) {this.numMac = numMac;}
    public String getNomeModelo() {return nomeModelo;}
    public void setNomeModelo(String nomeModelo) {this.nomeModelo = nomeModelo;}
    public String getNomeRegiao() {return nomeRegiao;}
    public void setNomeRegiao(String nomeRegiao) {this.nomeRegiao = nomeRegiao;}
    public String getSiglaRegiao() {return siglaRegiao;}
    public void setSiglaRegiao(String siglaRegiao) {this.siglaRegiao = siglaRegiao;}

    //toString()
    @Override
    public String toString() {
        return "Totem{" +
                "idTotem=" + idTotem +
                ", numMac='" + numMac + '\'' +
                ", nomeModelo='" + nomeModelo + '\'' +
                ", nomeRegiao='" + nomeRegiao + '\'' +
                ", siglaRegiao='" + siglaRegiao + '\'' +
                '}';
    }
}
