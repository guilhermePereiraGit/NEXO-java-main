package school.sptech;

import java.util.ArrayList;

public class AdministradorLog {

    private ArrayList<Log> logs;

    // Constrói AdministradorLog iniciando a lista
    public AdministradorLog() {
        logs = new ArrayList<>();
    }

    // Adiciona log na lista
    public void adicionarLog(Log log) {
        logs.add(log);
    }

    // Mostra todos os logs
    public void mostrarTodosLogs() {
        if (logs.isEmpty()) {
            System.out.println("Nenhum log registrado.");
        } else {
            for (Log log : logs) {
                System.out.println(log);
            }
        }
    }

    // Ordena logs por data (mais antigo primeiro) usando selection sort
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

    // Ordena logs por CPU, RAM ou Disco usando selection sort
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

    // Ordena logs por CPU
    public void ordenarPorCPU() {
        selectionSort(logs, "getUsoCPU");
    }

    // Ordena logs por RAM
    public void ordenarPorRAM() {
        selectionSort(logs, "getUsoRAM");
    }

    // Ordena logs por Disco
    public void ordenarPorDisco() {
        selectionSort(logs, "getUsoDisco");
    }

    // Mostra ID, data e CPU
    public void mostrarDataECpu() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | CPU: " + log.getUsoCPU() + "%");
        }
    }

    // Mostra ID, data e RAM
    public void mostrarDataERam() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | RAM: " + log.getUsoRAM() + "%");
        }
    }

    // Mostra ID, data e Disco
    public void mostrarDataEDisco() {
        for (Log log : logs) {
            System.out.println("ID: " + log.getIdentificadorTotem() + " | Data: " + log.getDataHora() + " | Disco: " + log.getUsoDisco() + "%");
        }
    }

    // Mostra logs com CPU acima do limite
    public void mostrarLogsCpuAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoCPU() > limite) {
                System.out.println(log);
                encontrou = true;
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com CPU acima de " + limite + "%.");
        }
    }

    // Mostra logs com RAM acima do limite
    public void mostrarLogsRamAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoRAM() > limite) {
                System.out.println(log);
                encontrou = true;
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com RAM acima de " + limite + "%.");
        }
    }

    // Mostra logs com Disco acima do limite
    public void mostrarLogsDiscoAcimaDe(double limite) {
        boolean encontrou = false;
        for (Log log : logs) {
            if (log.getUsoDisco() > limite) {
                System.out.println(log);
                encontrou = true;
            }
        }
        if (!encontrou) {
            System.out.println("Nenhum log com Disco acima de " + limite + "%.");
        }
    }
}