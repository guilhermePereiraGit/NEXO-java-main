package school.sptech;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Scanner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import java.util.HashSet;

public class TratamentoV3 {

    // ===== Limiares =====
    private static final Double LIMITE_CPU_SISTEMA   = 80.0;
    private static final Double LIMITE_RAM_SISTEMA   = 80.0;
    private static final Double LIMITE_DISCO_SISTEMA = 90.0;

    private static final Double LIMITE_CPU_PROCESSO   = 30.0;
    private static final Double LIMITE_RAM_PROCESSO   = 20.0;
    private static final Double LIMITE_DISCO_PROCESSO = 20.0;

    private static final String[] NOMES_SUSPEITOS = { "cryptominer.exe", "xmrig", "miner" };

    // ===== Caminhos =====
    private static final String CAMINHO_DADOS_RESOURCE     = "src/main/resources/Dados.csv";
    private static final String CAMINHO_DADOS_RAIZ         = "Dados.csv";
    private static final String CAMINHO_PROCESSOS_RESOURCE = "src/main/resources/Processos.csv";
    private static final String CAMINHO_PROCESSOS_RAIZ     = "Processos.csv";

    private static final String SAIDA_DADOS_V3     = "dados_tratados_v3.csv";
    private static final String SAIDA_PROCESSOS_V3 = "processos_tratados_v3.csv";

    // ===== Datas / Locale =====
    private static final DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Locale LOCALE_PTBR = new Locale("pt", "BR");

    // ===== Contadores =====
    private static Integer totalLeiturasSistema = 0;
    private static Integer totalOk = 0;
    private static Integer totalAtencao = 0;
    private static Integer totalPerigoso = 0;
    private static Integer totalMuitoPerigoso = 0;
    private static final Set<String> totensDistintosDados = new HashSet<>();

    private static Integer totalLinhasProcesso = 0;
    private static Integer totalAltoConsumo = 0;
    private static Integer totalSuspeitos = 0;
    private static Integer totalRiscoDuplo = 0;
    private static final Set<String> totensDistintosProcessos = new HashSet<>();
    private static final Set<String> totensComSuspeito = new HashSet<>();
    private static final Set<String> totensComAltoConsumo = new HashSet<>();
    private static final Set<String> totensComRiscoDuplo = new HashSet<>();

    public static void main(String[] args) {
        Long inicio = System.nanoTime();

        String caminhoDados = escolherCaminho(CAMINHO_DADOS_RESOURCE, CAMINHO_DADOS_RAIZ);
        String caminhoProcessos = escolherCaminho(CAMINHO_PROCESSOS_RESOURCE, CAMINHO_PROCESSOS_RAIZ);

        System.out.println("[1/3] Tratando métricas do sistema (v3)...");
        tratarDadosSistema(caminhoDados, SAIDA_DADOS_V3);

        System.out.println("[2/3] Tratando processos (v3)...");
        tratarProcessos(caminhoProcessos, SAIDA_PROCESSOS_V3);

        System.out.println("[3/3] Gerando resumo (v3)...");
        imprimirResumoConsole(inicio);
    }

    private static String escolherCaminho(String caminhoResource, String caminhoRaiz) {
        File res = new File(caminhoResource);
        if (res.exists()) return caminhoResource;
        File raiz = new File(caminhoRaiz);
        if (raiz.exists()) return caminhoRaiz;
        System.out.println("AVISO: Arquivo não encontrado: " + caminhoResource + " nem " + caminhoRaiz);
        return caminhoResource;
    }

    // =================== DADOS ===================
    private static void tratarDadosSistema(String caminhoEntrada, String caminhoSaida) {
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(caminhoEntrada)));
                PrintWriter escritor = new PrintWriter(new FileWriter(caminhoSaida))
        ) {
            Boolean primeira = true;
            escritor.println("timestamp,mac,cpu,ram,disco,qtd_processos,alerta_cpu,alerta_ram,alerta_disco,nivel");

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeira) { primeira = false; continue; }
                if (linha == null || linha.trim().isEmpty()) continue;

                String[] c = linha.split(",", -1);
                if (c.length < 6) continue;

                String tsBruto = c[0].trim();
                Double cpu = parseDouble(c[1]);
                Double ram = parseDouble(c[2]);
                Double disco = parseDouble(c[3]);
                Integer qtdProc = parseInteger(c[4]);
                String mac = c[5].trim();

                totensDistintosDados.add(mac);
                totalLeiturasSistema++;

                Boolean acCpu = cpu >= LIMITE_CPU_SISTEMA;
                Boolean acRam = ram >= LIMITE_RAM_SISTEMA;
                Boolean acDisco = disco >= LIMITE_DISCO_SISTEMA;

                String nivel = classificarNivel(acCpu, acRam, acDisco);

                if (nivel.equals("OK")) totalOk++;
                else if (nivel.equals("ATENCAO")) totalAtencao++;
                else if (nivel.equals("PERIGOSO")) totalPerigoso++;
                else totalMuitoPerigoso++;

                String ts = formatarData(tsBruto);

                escritor.println(ts + "," + mac + "," +
                        formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                        qtdProc + "," +
                        (acCpu ? "SIM" : "NAO") + "," +
                        (acRam ? "SIM" : "NAO") + "," +
                        (acDisco ? "SIM" : "NAO") + "," +
                        nivel);
            }
        } catch (Exception e) {
            System.out.println("Erro ao tratar dados (v3): " + e.getMessage());
        }
    }

    private static String classificarNivel(Boolean cpu, Boolean ram, Boolean disco) {
        int qtd = 0;
        if (cpu) qtd++;
        if (ram) qtd++;
        if (disco) qtd++;
        if (qtd == 0) return "OK";
        if (qtd == 1) return "ATENCAO";
        if (qtd == 2) return "PERIGOSO";
        return "MUITO_PERIGOSO";
    }

    // =================== PROCESSOS ===================
    private static void tratarProcessos(String caminhoEntrada, String caminhoSaida) {
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(caminhoEntrada)));
                PrintWriter escritor = new PrintWriter(new FileWriter(caminhoSaida))
        ) {
            Boolean primeira = true;
            escritor.println("timestamp,mac,processo,cpu,ram,disco,alerta_processo,motivo,tipo_alerta");

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeira) { primeira = false; continue; }
                if (linha == null || linha.trim().isEmpty()) continue;

                String[] c = linha.split(",", -1);
                if (c.length < 6) continue;

                String tsBruto = c[0].trim();
                String processo = c[1].trim();
                Double cpu = parseDouble(c[2]);
                Double ram = parseDouble(c[3]);
                Double disco = parseDouble(c[4]);
                String mac = c[5].trim();

                totensDistintosProcessos.add(mac);
                totalLinhasProcesso++;

                Boolean acCpu = cpu >= LIMITE_CPU_PROCESSO;
                Boolean acRam = ram >= LIMITE_RAM_PROCESSO;
                Boolean acDisco = disco >= LIMITE_DISCO_PROCESSO;

                Boolean altoConsumo = acCpu || acRam || acDisco;
                Boolean suspeito = ehSuspeito(processo);

                if (altoConsumo) totalAltoConsumo++;
                if (suspeito) totalSuspeitos++;
                if (altoConsumo && suspeito) totalRiscoDuplo++;

                if (suspeito) totensComSuspeito.add(mac);
                if (altoConsumo) totensComAltoConsumo.add(mac);
                if (altoConsumo && suspeito) totensComRiscoDuplo.add(mac);

                String tipoAlerta = definirTipoAlerta(altoConsumo, suspeito);
                String motivo = "-";
                if (suspeito && altoConsumo) motivo = "PROCESSO_SUSPEITO|ALTO_CONSUMO";
                else if (suspeito) motivo = "PROCESSO_SUSPEITO";
                else if (altoConsumo) motivo = "ALTO_CONSUMO";

                String ts = formatarData(tsBruto);

                escritor.println(ts + "," + mac + "," + processo.replace(",", " ") + "," +
                        formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                        ((altoConsumo || suspeito) ? "SIM" : "NAO") + "," +
                        motivo + "," + tipoAlerta);
            }
        } catch (Exception e) {
            System.out.println("Erro ao tratar processos (v3): " + e.getMessage());
        }
    }

    // =================== Apoio ===================
    private static String definirTipoAlerta(Boolean altoConsumo, Boolean suspeito) {
        if (altoConsumo && suspeito) return "AMBOS";
        if (suspeito) return "PROCESSO_SUSPEITO";
        if (altoConsumo) return "DESEMPENHO";
        return "-";
    }

    private static Boolean ehSuspeito(String nome) {
        if (nome == null) return false;
        String n = nome.toLowerCase();
        for (String termo : NOMES_SUSPEITOS) if (n.contains(termo)) return true;
        return false;
    }

    private static String formatarData(String tsBruto) {
        try {
            LocalDateTime dt = LocalDateTime.parse(tsBruto, FORMATO_ENTRADA);
            return dt.format(FORMATO_SAIDA);
        } catch (Exception e) {
            return tsBruto;
        }
    }

    private static Double parseDouble(String t) {
        try { return Double.valueOf(t.trim()); } catch (Exception e) { return 0.0; }
    }
    private static Integer parseInteger(String t) {
        try { return Integer.valueOf(t.trim()); } catch (Exception e) { return 0; }
    }
    private static String formatarPct(Double n) {
        return Math.round(n * 10.0) / 10.0 + "%";
    }
    private static String percentualTexto(Integer parte, Integer total, String sufixo) {
        if (total == 0) return " (n/a)";
        Double p = (parte * 100.0) / total;
        return " (" + String.format(LOCALE_PTBR, "%.1f", p) + "% " + sufixo + ")";
    }

    // =================== RESUMO ===================
    private static void imprimirResumoConsole(Long inicioNs) {
        System.out.println("====================================================");
        System.out.println("                 RESUMO GERAL (V3)                  ");
        System.out.println("====================================================");
        System.out.println("Totens distintos monitorados: " + totensDistintosDados.size());
        System.out.println("Leituras de sistema processadas: " + totalLeiturasSistema);
        System.out.println("----------------------------------------------------");
        System.out.println("Status das leituras:");
        System.out.println("OK: " + totalOk + " | Atenção: " + totalAtencao + " | Perigoso: " + totalPerigoso + " | Muito Perigoso: " + totalMuitoPerigoso);
        System.out.println("----------------------------------------------------");
        System.out.println("Processos monitorados (linhas em Processos.csv): " + totalLinhasProcesso);
        System.out.println("Processos com sobrecarga: " + totalAltoConsumo + percentualTexto(totalAltoConsumo, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("Processos suspeitos: " + totalSuspeitos + percentualTexto(totalSuspeitos, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("Processos com risco duplo: " + totalRiscoDuplo + percentualTexto(totalRiscoDuplo, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("----------------------------------------------------");
        System.out.println("Totens com ≥1 processo suspeito: " + totensComSuspeito.size() + percentualTexto(totensComSuspeito.size(), totensDistintosProcessos.size(), "dos totens com processos"));
        System.out.println("Totens com ≥1 processo de alto consumo: " + totensComAltoConsumo.size() + percentualTexto(totensComAltoConsumo.size(), totensDistintosProcessos.size(), "dos totens com processos"));
        System.out.println("Totens com risco duplo: " + totensComRiscoDuplo.size() + percentualTexto(totensComRiscoDuplo.size(), totensDistintosProcessos.size(), "dos totens com processos"));
        System.out.println("====================================================");
        System.out.println("Arquivos gerados:");
        System.out.println(" - " + SAIDA_DADOS_V3);
        System.out.println(" - " + SAIDA_PROCESSOS_V3);
        System.out.println("----------------------------------------------------");
        Double duracaoSeg = (System.nanoTime() - inicioNs) / 1_000_000_000.0;
        System.out.println("Tempo total de processamento: " + String.format(LOCALE_PTBR, "%.2f", duracaoSeg) + " s");
        System.out.println("====================================================");
    }
}