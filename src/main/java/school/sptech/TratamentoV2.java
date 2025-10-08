package school.sptech;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class TratamentoV2 {

    // ===== Limiares =====
    private static final Double LIMITE_CPU_SISTEMA   = 80.0;
    private static final Double LIMITE_RAM_SISTEMA   = 80.0;
    private static final Double LIMITE_DISCO_SISTEMA = 90.0;

    private static final Double LIMITE_CPU_PROCESSO   = 30.0;
    private static final Double LIMITE_RAM_PROCESSO   = 20.0;
    private static final Double LIMITE_DISCO_PROCESSO = 20.0;

    private static final String[] NOMES_SUSPEITOS = {
            "cryptominer.exe", "xmrig", "miner", "miner_helper", "miner_worker"
    };

    // ===== Caminhos =====
    private static final String CAMINHO_DADOS_RESOURCE     = "src/main/resources/Dados.csv";
    private static final String CAMINHO_DADOS_RAIZ         = "Dados.csv";
    private static final String CAMINHO_PROCESSOS_RESOURCE = "src/main/resources/Processos.csv";
    private static final String CAMINHO_PROCESSOS_RAIZ     = "Processos.csv";

    private static final String SAIDA_DADOS_V2                  = "dados_tratados_v2.csv";
    private static final String SAIDA_PROCESSOS_V2              = "processos_tratados_v2.csv";
    private static final String SAIDA_PROCESSOS_SOMENTE_SUS_V2  = "processos_suspeitos_v2.csv";
    private static final String SAIDA_PROCESSOS_RESUMO_TOTEM_V2 = "processos_resumo_por_totem_v2.csv";

    // ===== Datas / Locale =====
    private static final DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Locale LOCALE_PTBR = new Locale("pt", "BR");

    // ===== Contadores (Dados.csv) =====
    private static Integer totalLeiturasSistema = 0;
    private static Integer totalOk = 0;
    private static Integer totalAtencao = 0;
    private static Integer totalPerigoso = 0;
    private static Integer totalMuitoPerigoso = 0;

    // ===== Contadores (Processos.csv) =====
    private static Integer totalLinhasProcesso = 0;
    private static Integer totalAltoConsumo = 0;
    private static Integer totalSuspeitos = 0;
    private static Integer totalRiscoDuplo = 0;

    // ===== Totens =====
    private static final Set<String> totensComLeituraDados = new HashSet<>();
    private static final Set<String> totensEncontradosEmProcessos = new HashSet<>();
    private static final Set<String> totensComSuspeito = new HashSet<>();
    private static final Set<String> totensComAltoConsumo = new HashSet<>();
    private static final Set<String> totensComRiscoDuplo = new HashSet<>();

    // ===== Resumo por Totem =====
    private static final Map<String, Set<String>> nomesSuspeitosPorTotem = new HashMap<>();
    private static final Map<String, Integer> qtdSuspeitosPorTotem = new HashMap<>();
    private static final Map<String, Integer> qtdAltoConsumoPorTotem = new HashMap<>();
    private static final Map<String, Integer> qtdRiscoDuploPorTotem = new HashMap<>();
    private static final Map<String, String> primeiraOcorrenciaPorTotem = new HashMap<>();
    private static final Map<String, String> ultimaOcorrenciaPorTotem = new HashMap<>();

    public static void main(String[] args) {
        Long inicioProcessamentoNs = System.nanoTime();

        String caminhoDados = escolherCaminho(CAMINHO_DADOS_RESOURCE, CAMINHO_DADOS_RAIZ);
        String caminhoProcessos = escolherCaminho(CAMINHO_PROCESSOS_RESOURCE, CAMINHO_PROCESSOS_RAIZ);

        System.out.println("[1/3] Tratando métricas do sistema (v2)...");
        tratarDadosSistema(caminhoDados, SAIDA_DADOS_V2);

        System.out.println("[2/3] Tratando processos (v2)...");
        tratarProcessos(caminhoProcessos, SAIDA_PROCESSOS_V2, SAIDA_PROCESSOS_SOMENTE_SUS_V2, SAIDA_PROCESSOS_RESUMO_TOTEM_V2);

        System.out.println("[3/3] Gerando resumo (v2)...");
        imprimirResumoConsole(inicioProcessamentoNs);
    }

    private static String escolherCaminho(String caminhoResource, String caminhoRaiz) {
        File arqResource = new File(caminhoResource);
        if (arqResource.exists()) return caminhoResource;
        File arqRaiz = new File(caminhoRaiz);
        if (arqRaiz.exists()) return caminhoRaiz;
        System.out.println("AVISO: Arquivo não encontrado: " + caminhoResource + " nem " + caminhoRaiz);
        return caminhoResource;
    }

    // ===== DADOS.csv =====
    private static void tratarDadosSistema(String caminhoEntrada, String caminhoSaida) {
        Scanner leitor = null;
        PrintWriter escritor = null;

        try {
            leitor = new Scanner(new FileInputStream(new File(caminhoEntrada)));
            escritor = new PrintWriter(new FileWriter(caminhoSaida));

            Boolean primeira = true;
            escritor.println("timestamp,mac,nivel,cpu,ram,disco,CPU_acima_limite,RAM_acima_limite,DISCO_acima_limite,qtd_processos,resumo");

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
                Integer qtdProcessos = parseInteger(c[4]);
                String mac = c[5].trim();

                totensComLeituraDados.add(mac);

                String ts = formatarData(tsBruto);

                Boolean acCpu = cpu >= LIMITE_CPU_SISTEMA;
                Boolean acRam = ram >= LIMITE_RAM_SISTEMA;
                Boolean acDisco = disco >= LIMITE_DISCO_SISTEMA;

                String nivel = classificarNivel(acCpu, acRam, acDisco);

                totalLeiturasSistema++;
                if (nivel.equals("OK")) totalOk++;
                else if (nivel.equals("ATENCAO")) totalAtencao++;
                else if (nivel.equals("PERIGOSO")) totalPerigoso++;
                else totalMuitoPerigoso++;

                escritor.println(
                        ts + "," + mac + "," + nivel + "," +
                                formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                                (acCpu ? "SIM" : "NAO") + "," + (acRam ? "SIM" : "NAO") + "," + (acDisco ? "SIM" : "NAO") + "," +
                                qtdProcessos + "," +
                                (nivel.equals("OK") ? "TODOS OS PARÂMETROS DENTRO DO LIMITE." : "LIMITES EXCEDIDOS")
                );
            }
        } catch (Exception e) {
            System.out.println("Erro ao tratar dados do sistema: " + e.getMessage());
        } finally {
            if (leitor != null) leitor.close();
            if (escritor != null) escritor.close();
        }
    }

    private static String classificarNivel(Boolean acCpu, Boolean acRam, Boolean acDisco) {
        Integer qtd = 0;
        if (acCpu) qtd++;
        if (acRam) qtd++;
        if (acDisco) qtd++;
        if (qtd.equals(0)) return "OK";
        if (qtd.equals(1)) return "ATENCAO";
        if (qtd.equals(2)) return "PERIGOSO";
        return "MUITO_PERIGOSO";
    }

    // ===== PROCESSOS.csv =====
    private static void tratarProcessos(String caminhoEntrada, String saidaCompleto, String saidaSuspeitos, String saidaResumoTotem) {
        Scanner leitor = null;
        PrintWriter outCompleto = null;
        PrintWriter outSuspeitos = null;
        PrintWriter outResumo = null;

        try {
            leitor = new Scanner(new FileInputStream(new File(caminhoEntrada)));
            outCompleto = new PrintWriter(new FileWriter(saidaCompleto));
            outSuspeitos = new PrintWriter(new FileWriter(saidaSuspeitos));

            Boolean primeira = true;
            String cab = "timestamp,mac,processo,cpu,ram,disco,alerta_processo,motivo,tipo_alerta";
            outCompleto.println(cab);
            outSuspeitos.println(cab);

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

                totensEncontradosEmProcessos.add(mac);

                String ts = formatarData(tsBruto);

                Boolean acCpu = cpu >= LIMITE_CPU_PROCESSO;
                Boolean acRam = ram >= LIMITE_RAM_PROCESSO;
                Boolean acDisco = disco >= LIMITE_DISCO_PROCESSO;

                Boolean altoConsumo = acCpu || acRam || acDisco;
                Boolean suspeito = ehSuspeito(processo);

                String tipoAlerta = definirTipoAlerta(altoConsumo, suspeito);
                String motivo = "-";
                if (suspeito && altoConsumo) motivo = "PROCESSO_SUSPEITO|ALTO_CONSUMO";
                else if (suspeito) motivo = "PROCESSO_SUSPEITO";
                else if (altoConsumo) motivo = "ALTO_CONSUMO";

                totalLinhasProcesso++;
                if (altoConsumo) totalAltoConsumo++;
                if (suspeito) totalSuspeitos++;
                if (altoConsumo && suspeito) totalRiscoDuplo++;

                if (suspeito) totensComSuspeito.add(mac);
                if (altoConsumo) totensComAltoConsumo.add(mac);
                if (altoConsumo && suspeito) totensComRiscoDuplo.add(mac);

                outCompleto.println(
                        ts + "," + mac + "," + processo.replace(",", " ") + "," +
                                formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                                ((altoConsumo || suspeito) ? "SIM" : "NAO") + "," +
                                motivo + "," + tipoAlerta
                );

                if (suspeito) {
                    outSuspeitos.println(
                            ts + "," + mac + "," + processo.replace(",", " ") + "," +
                                    formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                                    "SIM,PROCESSO_SUSPEITO," + tipoAlerta
                    );
                }
            }

            outResumo = new PrintWriter(new FileWriter(saidaResumoTotem));
            outResumo.println("mac,qtde_processos_suspeitos,qtde_alertas_desempenho,qtde_riscos_duplos");
            Set<String> totensOrdenados = new TreeSet<>(totensEncontradosEmProcessos);
            for (String mac : totensOrdenados) {
                Integer qSus = qtdSuspeitosPorTotem.getOrDefault(mac, 0);
                Integer qDes = qtdAltoConsumoPorTotem.getOrDefault(mac, 0);
                Integer qAmb = qtdRiscoDuploPorTotem.getOrDefault(mac, 0);
                outResumo.println(mac + "," + qSus + "," + qDes + "," + qAmb);
            }

        } catch (Exception e) {
            System.out.println("Erro ao tratar processos: " + e.getMessage());
        } finally {
            if (leitor != null) leitor.close();
            if (outCompleto != null) outCompleto.close();
            if (outSuspeitos != null) outSuspeitos.close();
            if (outResumo != null) outResumo.close();
        }
    }

    // ===== Apoio =====
    private static String definirTipoAlerta(Boolean altoConsumo, Boolean suspeito) {
        if (altoConsumo && suspeito) return "AMBOS";
        if (suspeito) return "PROCESSO_SUSPEITO";
        if (altoConsumo) return "DESEMPENHO";
        return "-";
    }

    private static Boolean ehSuspeito(String nome) {
        if (nome == null) return false;
        String n = nome.toLowerCase();
        for (String termo : NOMES_SUSPEITOS) {
            if (n.contains(termo.toLowerCase())) return true;
        }
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
        try {
            if (t == null || t.trim().isEmpty()) return 0.0;
            return Double.valueOf(t.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static Integer parseInteger(String t) {
        try {
            if (t == null || t.trim().isEmpty()) return 0;
            return Integer.valueOf(t.trim());
        } catch (Exception e) {
            return 0;
        }
    }

    private static String formatarPct(Double n) {
        Double arred = Math.round(n * 10.0) / 10.0;
        return arred + "%";
    }

    private static String percentualFormatado(Integer parte, Integer total, String sufixo) {
        if (total == null || total <= 0) return " (n/a)";
        Double p = (parte * 100.0) / total;
        return " (" + String.format(LOCALE_PTBR, "%.1f", p) + "% " + sufixo + ")";
    }

    // ===== Resumo final (console) =====
    private static void imprimirResumoConsole(Long inicioNs) {
        System.out.println("====================================================");
        System.out.println("                 RESUMO GERAL (V2)                  ");
        System.out.println("====================================================");
        System.out.println("Totens distintos monitorados: " + totensComLeituraDados.size());
        System.out.println("Leituras de sistema processadas: " + totalLeiturasSistema);
        System.out.println("----------------------------------------------------");
        System.out.println("Status das leituras:");
        System.out.println("OK: " + totalOk + " | Atenção: " + totalAtencao + " | Perigoso: " + totalPerigoso + " | Muito Perigoso: " + totalMuitoPerigoso);
        System.out.println("----------------------------------------------------");
        System.out.println("Processos monitorados (linhas em Processos.csv): " + totalLinhasProcesso);
        System.out.println("Processos com sobrecarga: " + totalAltoConsumo
                + percentualFormatado(totalAltoConsumo, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("Processos suspeitos: " + totalSuspeitos
                + percentualFormatado(totalSuspeitos, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("Processos com risco duplo: " + totalRiscoDuplo
                + percentualFormatado(totalRiscoDuplo, totalLinhasProcesso, "do total de processos monitorados"));
        System.out.println("----------------------------------------------------");
        System.out.println("Totens com ≥1 processo suspeito: " + totensComSuspeito.size()
                + percentualFormatado(totensComSuspeito.size(), totensComLeituraDados.size(),
                "do total de totens monitorados"));
        System.out.println("Totens com ≥1 processo de alto consumo: " + totensComAltoConsumo.size()
                + percentualFormatado(totensComAltoConsumo.size(), totensComLeituraDados.size(),
                "do total de totens monitorados"));
        System.out.println("Totens com risco duplo: " + totensComRiscoDuplo.size()
                + percentualFormatado(totensComRiscoDuplo.size(), totensComLeituraDados.size(),
                "do total de totens monitorados"));
        System.out.println("====================================================");
        System.out.println("Arquivos gerados:");
        System.out.println(" - " + SAIDA_DADOS_V2);
        System.out.println(" - " + SAIDA_PROCESSOS_V2);
        System.out.println(" - " + SAIDA_PROCESSOS_SOMENTE_SUS_V2);
        System.out.println(" - " + SAIDA_PROCESSOS_RESUMO_TOTEM_V2);
        System.out.println("----------------------------------------------------");
        Double duracaoSeg = (System.nanoTime() - inicioNs) / 1_000_000_000.0;
        System.out.println("Tempo total de processamento: " + String.format(LOCALE_PTBR, "%.2f", duracaoSeg) + " s");
        System.out.println("====================================================");
    }
}