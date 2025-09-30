package school.sptech;

import java.lang.reflect.Method;

public class Log {
    private String identificadorTotem;
    private String dataHora;
    private Double usoCPU;
    private Double usoRAM;
    private Double usoDisco;

    public Log(String identificadorTotem, String dataHora, Double usoCPU, Double usoRAM, Double usoDisco) {
        this.identificadorTotem = identificadorTotem;
        this.dataHora = dataHora;
        this.usoCPU = usoCPU;
        this.usoRAM = usoRAM;
        this.usoDisco = usoDisco;
    }

    public String getIdentificadorTotem() {
        return identificadorTotem;
    }

    public String getDataHora() {
        return dataHora;
    }

    public Double getUsoCPU() {
        return usoCPU;
    }

    public Double getUsoRAM() {
        return usoRAM;
    }

    public Double getUsoDisco() {
        return usoDisco;
    }

    @Override
    public String toString() {
        return "ID: " + identificadorTotem +
                " | Data: " + dataHora +
                " | CPU: " + usoCPU + "%" +
                " | RAM: " + usoRAM + "%" +
                " | Disco: " + usoDisco + "%";
    }

    public Double executar(String comando) {
        try {
            Method metodo = this.getClass().getMethod(comando);
            return (Double) metodo.invoke(this);
        } catch (Exception e) {
            return 0.0;
        }
    }
}