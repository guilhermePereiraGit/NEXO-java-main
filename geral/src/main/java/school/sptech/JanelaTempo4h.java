package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class JanelaTempo4h {
    private List<Double> cpus = new ArrayList<>();
    private List<Double> rams = new ArrayList<>();
    private List<Double> discos = new ArrayList<>();
    private Double ultimoUptime = 0.0;
    private String modelo = "";
    private String empresa = "";

    void adicionarDado(Double cpu, Double ram, Double disco, Double uptime) {
        this.cpus.add(cpu);
        this.rams.add(ram);
        this.discos.add(disco);
        this.ultimoUptime = uptime;
    }

    double obterMediaCpu() {
        double soma = 0.0;

        for (int i = 0; i < cpus.size(); i++) {
            soma += cpus.get(i);
        }

        return soma / cpus.size();
    }

    double obterMediaRam() {
        double soma = 0.0;

        for (int i = 0; i < rams.size(); i++) {
            soma += rams.get(i);
        }

        return soma / rams.size();
    }

    double obterMediaDisco() {
        double soma = 0.0;

        for (int i = 0; i < discos.size(); i++) {
            soma += discos.get(i);
        }

        return soma / discos.size();
    }

    Double obterUltimoUptime() {
        return ultimoUptime;
    }

    public List<Double> getCpus() {
        return cpus;
    }

    public void setCpus(List<Double> cpus) {
        this.cpus = cpus;
    }

    public List<Double> getRams() {
        return rams;
    }

    public void setRams(List<Double> rams) {
        this.rams = rams;
    }

    public List<Double> getDiscos() {
        return discos;
    }

    public void setDiscos(List<Double> discos) {
        this.discos = discos;
    }

    public Double getUltimoUptime() {
        return ultimoUptime;
    }

    public void setUltimoUptime(Double ultimoUptime) {
        this.ultimoUptime = ultimoUptime;
    }

    public String getModelo() {
        return modelo;
    }

    public void setModelo(String modelo) {
        this.modelo = modelo;
    }

    public String getEmpresa() {
        return empresa;
    }

    public void setEmpresa(String empresa) {
        this.empresa = empresa;
    }
}
