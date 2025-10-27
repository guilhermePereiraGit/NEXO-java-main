package school.sptech;

import software.amazon.awssdk.services.s3.endpoints.internal.Value;

public class TipoParametro {
    private Integer idTipoParametro;
    private String componente;
    private String status;

    public TipoParametro() {
    }

    public TipoParametro(Integer idTipoParametro, String componente, String status) {
        this.idTipoParametro = idTipoParametro;
        this.componente = componente;
        this.status = status;
    }

    public Integer getIdTipoParametro() {
        return idTipoParametro;
    }

    public void setIdTipoParametro(Integer idTipoParametro) {
        this.idTipoParametro = idTipoParametro;
    }

    public String getComponente() {
        return componente;
    }

    public void setComponente(String componente) {
        this.componente = componente;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
