package school.sptech;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class EtlV1 {

    public static void main(String[] args) {
        String caminhoArquivoEntrada = "src/main/resources/logs.csv";
        String caminhoArquivoSaida = "logs-filtrados.csv";

        List<Log> listaTodos = lerArquivoCsv(caminhoArquivoEntrada);

        List<Log> listaFiltrados = new ArrayList<>();
        for (Log registro : listaTodos) {
            if (registro.getUsoCPU() != null && registro.getUsoCPU() > 70.0) {
                listaFiltrados.add(registro);
            }
        }

        listaFiltrados.sort(
                Comparator.comparing(Log::getUsoCPU).reversed()
        );

        List<Log> listaTop5 = new ArrayList<>();
        Integer quantidade = Math.min(5, listaFiltrados.size());
        for (Integer i = 0; i < quantidade; i++) {
            listaTop5.add(listaFiltrados.get(i));
        }

        gravarArquivoCsv(caminhoArquivoSaida, listaTop5);

        System.out.println("Lidos: " + listaTodos.size());
        System.out.println("Após filtro: " + listaFiltrados.size());
        System.out.println("Gravados: " + listaTop5.size());
        for (Log registro : listaTop5) {
            System.out.println(registro.getIdentificadorTotem() + " | " +
                    registro.getDataHora() + " | CPU=" + registro.getUsoCPU() +
                    " | RAM=" + registro.getUsoRAM() +
                    " | DISCO=" + registro.getUsoDisco());
        }
    }

    private static List<Log> lerArquivoCsv(String caminho) {
        List<Log> listaRegistros = new ArrayList<>();
        try (Scanner leitor = new Scanner(new FileInputStream(new File(caminho)))) {
            Boolean primeiraLinha = true;
            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeiraLinha) {
                    primeiraLinha = false;
                    continue;
                }
                if (linha == null || linha.trim().isEmpty()) continue;

                String[] colunas = linha.split(";");
                if (colunas.length < 5) continue;

                String idTotem = colunas[0];
                String dataHora = colunas[1];
                Double usoCpu = converterParaDouble(colunas[2]);
                Double usoRam = converterParaDouble(colunas[3]);
                Double usoDisco = converterParaDouble(colunas[4]);

                Log registro = new Log(idTotem, dataHora, usoCpu, usoRam, usoDisco);
                listaRegistros.add(registro);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Arquivo não encontrado: " + caminho);
        } catch (Exception e) {
            System.out.println("Erro ao ler o arquivo: " + e.getMessage());
        }
        return listaRegistros;
    }

    private static void gravarArquivoCsv(String caminho, List<Log> lista) {
        try (PrintWriter escritor = new PrintWriter(new FileWriter(caminho))) {
            escritor.println("TOTEM;DATA_HORA;CPU;RAM;DISCO");
            for (Log registro : lista) {
                escritor.println(tratarNulo(registro.getIdentificadorTotem()) + ";" +
                        tratarNulo(registro.getDataHora()) + ";" +
                        formatarDecimal(registro.getUsoCPU()) + ";" +
                        formatarDecimal(registro.getUsoRAM()) + ";" +
                        formatarDecimal(registro.getUsoDisco()));
            }
        } catch (IOException e) {
            System.out.println("Erro ao gravar arquivo: " + e.getMessage());
        }
    }

    private static Double converterParaDouble(String texto) {
        if (texto == null) return 0.0;
        String valor = texto.trim().replace(",", ".");
        if (valor.isEmpty()) return 0.0;
        try {
            return Double.valueOf(valor);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static String formatarDecimal(Double numero) {
        if (numero == null) return "";
        Double arredondado = Math.round(numero * 10.0) / 10.0;
        return String.valueOf(arredondado);
    }

    private static String tratarNulo(String texto) {
        return (texto == null) ? "" : texto;
    }
}