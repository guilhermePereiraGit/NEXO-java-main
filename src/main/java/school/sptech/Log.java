package school.sptech;

import java.lang.reflect.Method;

public class Log {
    private String identificadorTotem;
    private String dataHora;
    private double usoCPU;
    private double usoRAM;
    private double usoDisco;

    // Constrói Log com os dados informados
    public Log(String identificadorTotem, String dataHora, double usoCPU, double usoRAM, double usoDisco) {
        this.identificadorTotem = identificadorTotem;
        this.dataHora = dataHora;
        this.usoCPU = usoCPU;
        this.usoRAM = usoRAM;
        this.usoDisco = usoDisco;
    }

    // Retorna identificador do totem
    public String getIdentificadorTotem() {
        return identificadorTotem;
    }

    // Retorna data e hora do log
    public String getDataHora() {
        return dataHora;
    }

    // Retorna uso de CPU
    public double getUsoCPU() {
        return usoCPU;
    }

    // Retorna uso de RAM
    public double getUsoRAM() {
        return usoRAM;
    }

    // Retorna uso de Disco
    public double getUsoDisco() {
        return usoDisco;
    }

    // Retorna log formatado em texto
    public String toString() {
        return "ID: " + identificadorTotem +
                " | Data: " + dataHora +
                " | CPU: " + usoCPU + "%" +
                " | RAM: " + usoRAM + "%" +
                " | Disco: " + usoDisco + "%";
    }

    // Função para retornar o valor de um getter pelo nome (reflection)
    public Double executar(String comando) {
        try {
            Method metodo = this.getClass().getMethod(comando);
            return (Double) metodo.invoke(this);
        } catch (Exception e) {
            return 0.0;
        }
    }
}