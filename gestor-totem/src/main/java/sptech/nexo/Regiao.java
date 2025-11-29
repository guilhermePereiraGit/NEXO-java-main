package sptech.nexo;

public class Regiao {
    //Atributos
    private String nomeRegiao;
    private String sigla;

    //Construtores Cheio e Vazio
    public Regiao(Integer idRegiao, String nomeRegiao, String sigla) {
        this.nomeRegiao = nomeRegiao;
        this.sigla = sigla;
    }
    public Regiao() {}

    //Getters e Setters
    public String getNomeRegiao() {return nomeRegiao;}
    public void setNomeRegiao(String nomeRegiao) {this.nomeRegiao = nomeRegiao;}
    public String getSigla() {return sigla;}
    public void setSigla(String sigla) {this.sigla = sigla;}

    //toString()
    @Override
    public String toString() {
        return "Regiao{" +
                ", nomeRegiao='" + nomeRegiao + '\'' +
                ", sigla='" + sigla + '\'' +
                '}';
    }
}
