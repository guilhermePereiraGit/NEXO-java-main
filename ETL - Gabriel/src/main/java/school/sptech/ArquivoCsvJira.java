package school.sptech;

import com.opencsv.bean.CsvBindByName;

public class ArquivoCsvJira {

    // Variáveis de instância
    @CsvBindByName(column = "data_hora_aberto")
    private String data_hora_aberto;

    @CsvBindByName(column = "data_hora_finalizado")
    private String data_hora_finalizado;

    @CsvBindByName(column = "id_empresa")
    private Integer id_empresa;

    @CsvBindByName(column = "mac_totem")
    private String mac_totem;

    // Construtores
    public ArquivoCsvJira(){}

    public ArquivoCsvJira(String data_hora_aberto, String data_hora_finalizado, Integer id_empresa, String mac_totem) {
        this.data_hora_aberto = data_hora_aberto;
        this.data_hora_finalizado = data_hora_finalizado;
        this.id_empresa = id_empresa;
        this.mac_totem = mac_totem;
    }

    // Getters e Setters
    public String getData_hora_aberto() {
        return data_hora_aberto;
    }

    public void setData_hora_aberto(String data_hora_aberto) {
        this.data_hora_aberto = data_hora_aberto;
    }

    public String getData_hora_finalizado() {
        return data_hora_finalizado;
    }

    public void setData_hora_finalizado(String data_hora_finalizado) {
        this.data_hora_finalizado = data_hora_finalizado;
    }

    public Integer getId_empresa() {
        return id_empresa;
    }

    public void setId_empresa(Integer id_empresa) {
        this.id_empresa = id_empresa;
    }

    public String getId_totem() {
        return mac_totem;
    }

    public void setId_totem(String id_totem) {
        this.mac_totem = id_totem;
    }

    //toString
    @Override
    public String toString(){
        return  "Id da Empresa: " + id_empresa + "\n" +
                "MAC do Totem: " + mac_totem + "\n" +
                "Data e Hora de Abertura: " + data_hora_aberto + "\n" +
                "Data e Hora de Finalização: " + data_hora_finalizado + "\n";
    }
}
