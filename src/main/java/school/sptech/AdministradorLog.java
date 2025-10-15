package school.sptech;

import java.util.ArrayList;

public class AdministradorLog {

    private ArrayList<Log> logs;
    private ServicoAlerta servicoAlerta;
    private Double LIMITE_CPU = 80.0; // LIMITES PARA ALERTAS
    private Double LIMITE_RAM = 85.0;
    private Double LIMITE_DISCO = 90.0;

    // Constrói AdministradorLog iniciando a lista
    public AdministradorLog() {
        logs = new ArrayList<>();
        this.servicoAlerta = null; // INICIALMENTE NULL
    }

    public void setServicoAlerta(ServicoAlerta servicoAlerta) {
        this.servicoAlerta = servicoAlerta;
    }

    public void adicionarLog(Log log) {
        logs.add(log);

        if (servicoAlerta != null) {
            servicoAlerta.registrarAlertaCPU(log.getIdentificadorTotem(), log.getUsoCPU(), LIMITE_CPU);
            servicoAlerta.registrarAlertaRAM(log.getIdentificadorTotem(), log.getUsoRAM(), LIMITE_RAM);
            servicoAlerta.registrarAlertaDisco(log.getIdentificadorTotem(), log.getUsoDisco(), LIMITE_DISCO);
        }
    }

    public void mostrarLogsCpuAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoCPU() > limite) {
                System.out.println(log);
                encontrou = true;
                // ALERTA SE FOR CRÍTICO (NOVO)
                if (servicoAlerta != null && log.getUsoCPU() > LIMITE_CPU) {
                    servicoAlerta.registrarAlertaCPU(log.getIdentificadorTotem(), log.getUsoCPU(), LIMITE_CPU);
                }
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com CPU acima de " + limite + "%.");
        }
    }

    public void mostrarLogsRamAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoRAM() > limite) {
                System.out.println(log);
                encontrou = true;
                if (servicoAlerta != null && log.getUsoRAM() > LIMITE_RAM) {
                    servicoAlerta.registrarAlertaRAM(log.getIdentificadorTotem(), log.getUsoRAM(), LIMITE_RAM);
                }
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com RAM acima de " + limite + "%.");
        }
    }

    public void mostrarLogsDiscoAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoDisco() > limite) {
                System.out.println(log);
                encontrou = true;
                if (servicoAlerta != null && log.getUsoDisco() > LIMITE_DISCO) {
                    servicoAlerta.registrarAlertaDisco(log.getIdentificadorTotem(), log.getUsoDisco(), LIMITE_DISCO);
                }
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com Disco acima de " + limite + "%.");
        }
    }

    public void mostrarTodosLogs() {
        if (logs.isEmpty()) {
            System.out.println("Nenhum log registrado.");
        } else {
            for (Log log : logs) {
                System.out.println(log);
            }
        }
    }

    public void ordenarPorData() {
        for (int i = 0; i < logs.size() - 1; i++) {
            int indiceMenor = i;
            for (int j = i + 1; j < logs.size(); j++) {
                if (logs.get(j).getDataHora().compareTo(logs.get(indiceMenor).getDataHora()) < 0) {
                    indiceMenor = j;
                }
            }
            if (i != indiceMenor) {
                Log temp = logs.get(i);
                logs.set(i, logs.get(indiceMenor));
                logs.set(indiceMenor, temp);
            }
        }
        for (Log log : logs) {
            System.out.println(log);
        }
    }

    private void selectionSort(ArrayList<Log> logs, String componente) {
        for (int i = 0; i < logs.size() - 1; i++) {
            int indiceMenor = i;
            for (int j = i + 1; j < logs.size(); j++) {
                if (logs.get(j).executar(componente) < logs.get(indiceMenor).executar(componente)) {
                    indiceMenor = j;
                }
            }
            if (i != indiceMenor) {
                Log aux = logs.get(i);
                logs.set(i, logs.get(indiceMenor));
                logs.set(indiceMenor, aux);
            }
        }
        for (Log log : logs) {
            System.out.println(log);
        }
    }

    public void ordenarPorCPU() {
        selectionSort(logs, "getUsoCPU");
    }

    public void ordenarPorRAM() {
        selectionSort(logs, "getUsoRAM");
    }

    public void ordenarPorDisco() {
        selectionSort(logs, "getUsoDisco");
    }

    public void mostrarDataECpu() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | CPU: " + log.getUsoCPU() + "%");
        }
    }

    public void mostrarDataERam() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | RAM: " + log.getUsoRAM() + "%");
        }
    }

    public void mostrarDataEDisco() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | Disco: " + log.getUsoDisco() + "%");
        }
    }
}