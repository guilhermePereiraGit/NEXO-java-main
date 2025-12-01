package sptech.nexo;

import org.h2.engine.Mode;

public class Totem {
    //Atributos
    private Integer idTotem;
    private String numMac;
    private Regiao regiao;
    private Modelo modelo;

    //Construtor Cheio e Vazio
    public Totem(Integer idTotem, String numMac, Regiao regiao, Modelo modelo) {
        this.idTotem = idTotem;
        this.numMac = numMac;
        this.regiao = regiao;
        this.modelo = modelo;
    }

    public Totem() {}

    //Getters e Setters
    public Integer getIdTotem() {return idTotem;}
    public void setIdTotem(Integer idTotem) {this.idTotem = idTotem;}
    public String getNumMac() {return numMac;}
    public void setNumMac(String numMac) {this.numMac = numMac;}
    public Modelo getModelo() {return modelo;}
    public void setModelo(Modelo modelo) {this.modelo = modelo;}
    public Regiao getRegiao() {return regiao;}
    public void setRegiao(Regiao regiao) {this.regiao = regiao;}

    //toString()
    @Override
    public String toString() {
        return "Totem{" +
                "idTotem=" + idTotem +
                ", numMac='" + numMac + '\'' +
                ", regiao=" + regiao +
                ", modelo=" + modelo +
                '}';
    }
}
