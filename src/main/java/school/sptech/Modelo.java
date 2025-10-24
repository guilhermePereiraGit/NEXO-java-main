package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class Modelo {
    private Integer idModelo;
    private String nome;
    private String criador;
    private String tipo;
    private String descricaoArquitetura;
    private String status;
    private Integer fkEmpresa;
    private List<Parametro> parametros = new ArrayList<>();

    public Modelo() {
    }

    public Modelo(Integer idModelo, String nome, String criador, String tipo,
                  String descricaoArquitetura, String status, Integer fkEmpresa) {
        this.idModelo = idModelo;
        this.nome = nome;
        this.criador = criador;
        this.tipo = tipo;
        this.descricaoArquitetura = descricaoArquitetura;
        this.status = status;
        this.fkEmpresa = fkEmpresa;
        this.parametros = new ArrayList<>();
    }

    public Modelo(Integer idModelo, String nome, String criador, String tipo,
                  String descricaoArquitetura, String status, Integer fkEmpresa, List<Parametro> parametros) {
        this.idModelo = idModelo;
        this.nome = nome;
        this.criador = criador;
        this.tipo = tipo;
        this.descricaoArquitetura = descricaoArquitetura;
        this.status = status;
        this.fkEmpresa = fkEmpresa;
        this.parametros = parametros;
    }

    public Integer getIdModelo() { return idModelo; }
    public void setIdModelo(Integer idModelo) { this.idModelo = idModelo; }

    public String getNome() { return nome; }
    public void setNome(String nome) { this.nome = nome; }

    public String getCriador() { return criador; }
    public void setCriador(String criador) { this.criador = criador; }

    public String getTipo() { return tipo; }
    public void setTipo(String tipo) { this.tipo = tipo; }

    public String getDescricaoArquitetura() { return descricaoArquitetura; }
    public void setDescricaoArquitetura(String descricaoArquitetura) { this.descricaoArquitetura = descricaoArquitetura; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getFkEmpresa() { return fkEmpresa; }
    public void setFkEmpresa(Integer fkEmpresa) { this.fkEmpresa = fkEmpresa; }

    public List<Parametro> getParametros() { return parametros; }
    public void setParametros(List<Parametro> parametros) { this.parametros = parametros; }

    @Override
    public String toString() {
        return "Modelo{" +
                "idModelo=" + idModelo +
                ", nome='" + nome + '\'' +
                ", criador='" + criador + '\'' +
                ", tipo='" + tipo + '\'' +
                ", descricaoArquitetura='" + descricaoArquitetura + '\'' +
                ", status='" + status + '\'' +
                ", fkEmpresa=" + fkEmpresa +
                ", parametros=" + parametros +
                '}';
    }
}
