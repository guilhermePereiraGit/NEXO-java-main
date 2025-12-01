package sptech.nexo;

import java.util.ArrayList;
import java.util.List;

public class Modelo {
    //Atributos
    private Integer idModelo;
    private String nome;
    private List<Parametro> parametros = new ArrayList<>();
    private Integer fkEmpresa;

    //Construtores Cheio e Vazio
    public Modelo(Integer idModelo, String nome, List<Parametro> parametros, Integer fkEmpresa) {
        this.idModelo = idModelo;
        this.nome = nome;
        this.parametros = parametros;
        this.fkEmpresa = fkEmpresa;
    }

    public Modelo() {}

    public Integer getIdModelo() { return idModelo; }
    public void setIdModelo(Integer idModelo) { this.idModelo = idModelo; }
    public String getNome() { return nome; }
    public void setNome(String nome) { this.nome = nome; }
    public List<Parametro> getParametros() { return parametros; }
    public void setParametros(List<Parametro> parametros) { this.parametros = parametros; }
    public Integer getFkEmpresa() {return fkEmpresa;}
    public void setFkEmpresa(Integer fkEmpresa) {this.fkEmpresa = fkEmpresa;}

    @Override
    public String toString() {
        return "Modelo{" +
                "idModelo=" + idModelo +
                ", nome='" + nome + '\'' +
                ", parametros=" + parametros +
                '}';
    }
}
