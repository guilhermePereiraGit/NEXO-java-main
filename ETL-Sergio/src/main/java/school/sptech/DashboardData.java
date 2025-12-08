package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class DashboardData {

    // Informações gerais do modelo
    private String nomeModelo;
    private Integer totalMonitorado;          // Totens desse modelo
    private Integer totalTotensEmpresa;       // Totens da empresa inteira
    private Double percentualModeloParque;    // % que esse modelo representa no parque

    // KPI - equipamentos críticos
    private Integer totalCriticos;
    private Integer percentualCritico;

    // Totais de alertas por componente (geral)
    private Integer alertasCpu;
    private Integer alertasRam;
    private Integer alertasDisco;
    private Integer alertasProcessos;
    private Integer totalAlertas;

    // Variação de alertas entre o mês atual e o anterior
    private Double variacaoAlertas;

    // Limites de cada componente (vêm do banco)
    private Double limiteMinCpu;
    private Double limiteMaxCpu;

    private Double limiteMinRam;
    private Double limiteMaxRam;

    private Double limiteMinDisco;
    private Double limiteMaxDisco;

    private Double limiteMinProc;
    private Double limiteMaxProc;

    // Gráfico anual (12 meses)
    private List<String> meses = new ArrayList<>();
    private List<Integer> totalMensalCpu = new ArrayList<>();
    private List<Integer> totalMensalRam = new ArrayList<>();
    private List<Integer> totalMensalDisco = new ArrayList<>();
    private List<Integer> totalMensalProcessos = new ArrayList<>();

    // Gráfico de 30 dias
    private List<String> dias30 = new ArrayList<>();
    private List<Integer> totalMensal30Cpu = new ArrayList<>();
    private List<Integer> totalMensal30Ram = new ArrayList<>();
    private List<Integer> totalMensal30Disco = new ArrayList<>();
    private List<Integer> totalMensal30Processos = new ArrayList<>();

    // ================= GETTERS / SETTERS =================

    public String getNomeModelo() {
        return nomeModelo;
    }

    public void setNomeModelo(String nomeModelo) {
        this.nomeModelo = nomeModelo;
    }

    public Integer getTotalMonitorado() {
        return totalMonitorado;
    }

    public void setTotalMonitorado(Integer totalMonitorado) {
        this.totalMonitorado = totalMonitorado;
    }

    public Integer getTotalTotensEmpresa() {
        return totalTotensEmpresa;
    }

    public void setTotalTotensEmpresa(Integer totalTotensEmpresa) {
        this.totalTotensEmpresa = totalTotensEmpresa;
    }

    public Double getPercentualModeloParque() {
        return percentualModeloParque;
    }

    public void setPercentualModeloParque(Double percentualModeloParque) {
        this.percentualModeloParque = percentualModeloParque;
    }

    public Integer getTotalCriticos() {
        return totalCriticos;
    }

    public void setTotalCriticos(Integer totalCriticos) {
        this.totalCriticos = totalCriticos;
    }

    public Integer getPercentualCritico() {
        return percentualCritico;
    }

    public void setPercentualCritico(Integer percentualCritico) {
        this.percentualCritico = percentualCritico;
    }

    public Integer getAlertasCpu() {
        return alertasCpu;
    }

    public void setAlertasCpu(Integer alertasCpu) {
        this.alertasCpu = alertasCpu;
    }

    public Integer getAlertasRam() {
        return alertasRam;
    }

    public void setAlertasRam(Integer alertasRam) {
        this.alertasRam = alertasRam;
    }

    public Integer getAlertasDisco() {
        return alertasDisco;
    }

    public void setAlertasDisco(Integer alertasDisco) {
        this.alertasDisco = alertasDisco;
    }

    public Integer getAlertasProcessos() {
        return alertasProcessos;
    }

    public void setAlertasProcessos(Integer alertasProcessos) {
        this.alertasProcessos = alertasProcessos;
    }

    public Integer getTotalAlertas() {
        return totalAlertas;
    }

    public void setTotalAlertas(Integer totalAlertas) {
        this.totalAlertas = totalAlertas;
    }

    public Double getVariacaoAlertas() {
        return variacaoAlertas;
    }

    public void setVariacaoAlertas(Double variacaoAlertas) {
        this.variacaoAlertas = variacaoAlertas;
    }

    public Double getLimiteMinCpu() {
        return limiteMinCpu;
    }

    public void setLimiteMinCpu(Double limiteMinCpu) {
        this.limiteMinCpu = limiteMinCpu;
    }

    public Double getLimiteMaxCpu() {
        return limiteMaxCpu;
    }

    public void setLimiteMaxCpu(Double limiteMaxCpu) {
        this.limiteMaxCpu = limiteMaxCpu;
    }

    public Double getLimiteMinRam() {
        return limiteMinRam;
    }

    public void setLimiteMinRam(Double limiteMinRam) {
        this.limiteMinRam = limiteMinRam;
    }

    public Double getLimiteMaxRam() {
        return limiteMaxRam;
    }

    public void setLimiteMaxRam(Double limiteMaxRam) {
        this.limiteMaxRam = limiteMaxRam;
    }

    public Double getLimiteMinDisco() {
        return limiteMinDisco;
    }

    public void setLimiteMinDisco(Double limiteMinDisco) {
        this.limiteMinDisco = limiteMinDisco;
    }

    public Double getLimiteMaxDisco() {
        return limiteMaxDisco;
    }

    public void setLimiteMaxDisco(Double limiteMaxDisco) {
        this.limiteMaxDisco = limiteMaxDisco;
    }

    public Double getLimiteMinProc() {
        return limiteMinProc;
    }

    public void setLimiteMinProc(Double limiteMinProc) {
        this.limiteMinProc = limiteMinProc;
    }

    public Double getLimiteMaxProc() {
        return limiteMaxProc;
    }

    public void setLimiteMaxProc(Double limiteMaxProc) {
        this.limiteMaxProc = limiteMaxProc;
    }

    public List<String> getMeses() {
        return meses;
    }

    public void setMeses(List<String> meses) {
        this.meses = meses;
    }

    public List<Integer> getTotalMensalCpu() {
        return totalMensalCpu;
    }

    public void setTotalMensalCpu(List<Integer> totalMensalCpu) {
        this.totalMensalCpu = totalMensalCpu;
    }

    public List<Integer> getTotalMensalRam() {
        return totalMensalRam;
    }

    public void setTotalMensalRam(List<Integer> totalMensalRam) {
        this.totalMensalRam = totalMensalRam;
    }

    public List<Integer> getTotalMensalDisco() {
        return totalMensalDisco;
    }

    public void setTotalMensalDisco(List<Integer> totalMensalDisco) {
        this.totalMensalDisco = totalMensalDisco;
    }

    public List<Integer> getTotalMensalProcessos() {
        return totalMensalProcessos;
    }

    public void setTotalMensalProcessos(List<Integer> totalMensalProcessos) {
        this.totalMensalProcessos = totalMensalProcessos;
    }

    public List<String> getDias30() {
        return dias30;
    }

    public void setDias30(List<String> dias30) {
        this.dias30 = dias30;
    }

    public List<Integer> getTotalMensal30Cpu() {
        return totalMensal30Cpu;
    }

    public void setTotalMensal30Cpu(List<Integer> totalMensal30Cpu) {
        this.totalMensal30Cpu = totalMensal30Cpu;
    }

    public List<Integer> getTotalMensal30Ram() {
        return totalMensal30Ram;
    }

    public void setTotalMensal30Ram(List<Integer> totalMensal30Ram) {
        this.totalMensal30Ram = totalMensal30Ram;
    }

    public List<Integer> getTotalMensal30Disco() {
        return totalMensal30Disco;
    }

    public void setTotalMensal30Disco(List<Integer> totalMensal30Disco) {
        this.totalMensal30Disco = totalMensal30Disco;
    }

    public List<Integer> getTotalMensal30Processos() {
        return totalMensal30Processos;
    }

    public void setTotalMensal30Processos(List<Integer> totalMensal30Processos) {
        this.totalMensal30Processos = totalMensal30Processos;
    }
}
