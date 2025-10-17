package school.sptech;

public class TipoParametro {
    private Integer idTipoParametro;
    private String nome;

    public TipoParametro() {
    }

    public TipoParametro(Integer idTipoParametro, String nome) {
        this.idTipoParametro = idTipoParametro;
        this.nome = nome;
    }

    public Integer getIdTipoParametro() { return idTipoParametro; }
    public void setIdTipoParametro(Integer idTipoParametro) { this.idTipoParametro = idTipoParametro; }

    public String getNome() { return nome; }
    public void setNome(String nome) { this.nome = nome; }
}
