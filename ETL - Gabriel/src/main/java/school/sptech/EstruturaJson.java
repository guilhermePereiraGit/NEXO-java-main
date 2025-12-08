package school.sptech;

import school.sptech.records.*;
import java.util.List;

public class EstruturaJson {

    // Variáveis de instância
    private List<TecnicosRegiao> totalTecnicos;
    private List<ComparacaoRegiao> comparacao;
    private List<HorasIdeais> horasIdeais;
    private List<HorasTrabalhadas> horasTrabalhadas;
    private List<FuncionariosRecomendados> qtdRecomendadaDeFuncionarios;

    // Construtores
    public EstruturaJson(){}

    // Getters e Setters
    public List<ComparacaoRegiao> getComparacao() {
        return comparacao;
    }

    public void setComparacao(List<ComparacaoRegiao> comparacao) {
        this.comparacao = comparacao;
    }

    public List<HorasIdeais> getHorasIdeais() {
        return horasIdeais;
    }

    public void setHorasIdeais(List<HorasIdeais> horasIdeais) {
        this.horasIdeais = horasIdeais;
    }

    public List<HorasTrabalhadas> getHorasTrabalhadas() {
        return horasTrabalhadas;
    }

    public void setHorasTrabalhadas(List<HorasTrabalhadas> horasTrabalhadas) {
        this.horasTrabalhadas = horasTrabalhadas;
    }

    public List<FuncionariosRecomendados> getQtdRecomendadaDeFuncionarios() {
        return qtdRecomendadaDeFuncionarios;
    }

    public void setQtdRecomendadaDeFuncionarios(List<FuncionariosRecomendados> qtdRecomendadaDeFuncionarios) {
        this.qtdRecomendadaDeFuncionarios = qtdRecomendadaDeFuncionarios;
    }

    public List<TecnicosRegiao> getTotalTecnicos() {
        return totalTecnicos;
    }

    public void setTotalTecnicos(List<TecnicosRegiao> totalTecnicos) {
        this.totalTecnicos = totalTecnicos;
    }
}