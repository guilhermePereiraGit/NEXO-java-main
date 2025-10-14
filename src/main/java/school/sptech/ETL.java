package school.sptech;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Scanner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

/**
 * ============================================================
 *                          ETL
 * ------------------------------------------------------------
 * Simulação de buckets S3 localmente:
 *   RAW (resources) -> TRUSTED (storage/trusted) -> CLIENT (storage/client + cópia na raiz)
 *
 * RAW
 *   - src/main/resources/Dados.csv
 *   - src/main/resources/Processos.csv
 *
 * TRUSTED
 *   - storage/trusted/Dados_Trusted.csv
 *   - storage/trusted/Processos_Trusted.csv
 *
 * CLIENT
 *   - storage/client/Dados_Client.csv
 *   - storage/client/Processos_Client.csv
 *   - cópias na raiz: Dados_Client.csv / Processos_Client.csv (facilitam a demonstração)
 * ============================================================
 */
public class ETL {

    // 1) LIMITES / PARÂMETROS

    private static final Double LIMITE_CPU_SISTEMA   = 6.5;   // %
    private static final Double LIMITE_RAM_SISTEMA   = 82.0;  // %
    private static final Double LIMITE_DISCO_SISTEMA = 39.0;  // %

    private static final Double LIMITE_CPU_PROCESSO   = 1.0;   // %
    private static final Double LIMITE_RAM_PROCESSO   = 5.0;   // %
    private static final Double LIMITE_DISCO_PROCESSO = 300.0; // MB escritos

    private static final String[] PROCESSOS_SUSPEITOS = {
            "MemCompression", "Discord.exe", "Code.exe"
    };

    // 2) CAMINHOS (RAW/SAÍDAS)

    // Onde procuramos os CSVs brutos (RAW): primeiro resources, depois raiz
    private static final String CAMINHO_DADOS_RESOURCE     = "src/main/resources/Dados.csv";
    private static final String CAMINHO_DADOS_RAIZ         = "Dados.csv";
    private static final String CAMINHO_PROCESSOS_RESOURCE = "src/main/resources/Processos.csv";
    private static final String CAMINHO_PROCESSOS_RAIZ     = "Processos.csv";

    // Pastas “simulando” buckets S3
    private static final String PASTA_TRUSTED = "storage/trusted";
    private static final String PASTA_CLIENT  = "storage/client";

    // Saídas TRUSTED
    private static final String SAIDA_DADOS_TRUSTED     = PASTA_TRUSTED + "/Dados_Trusted.csv";
    private static final String SAIDA_PROCESSOS_TRUSTED = PASTA_TRUSTED + "/Processos_Trusted.csv";

    // Saídas CLIENT
    private static final String SAIDA_DADOS_CLIENT_PASTA     = PASTA_CLIENT + "/Dados_Client.csv";
    private static final String SAIDA_PROCESSOS_CLIENT_PASTA = PASTA_CLIENT + "/Processos_Client.csv";
    private static final String SAIDA_DADOS_CLIENT_RAIZ      = "Dados_Client.csv";
    private static final String SAIDA_PROCESSOS_CLIENT_RAIZ  = "Processos_Client.csv";

    // 3) FORMATO DE DATA — Python usa underline

    private static final DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    // 4) CONTADORES e “conjuntos” (sem duplicatas)

    // Contadores gerais do SISTEMA (linhas de Dados.csv tratadas):
    private static Integer qtdLeiturasSistema = 0;
    private static Integer qtdOk = 0;
    private static Integer qtdAtencao = 0;
    private static Integer qtdPerigoso = 0;
    private static Integer qtdMuitoPerigoso = 0;

    // Contadores gerais de PROCESSOS:
    private static Integer qtdLinhasProcesso = 0;
    private static Integer qtdAltoConsumo = 0;
    private static Integer qtdSuspeitos = 0;
    private static Integer qtdRiscoDuplo = 0;

    // “Conjuntos” de totens (sem duplicar MAC) — ArrayList + contains()
    private static final ArrayList<String> totensSistema        = new ArrayList<>();
    private static final ArrayList<String> totensProcessos      = new ArrayList<>();
    private static final ArrayList<String> totensComSuspeito    = new ArrayList<>();
    private static final ArrayList<String> totensComAltoConsumo = new ArrayList<>();
    private static final ArrayList<String> totensComRiscoDuplo  = new ArrayList<>();

    // 5) RAW -> TRUSTED

    /**
     * Lê Dados.csv bruto e grava Dados_Trusted.csv:
     * - Troca ';' por ','
     * - Normaliza números (troca vírgula por ponto; vazio -> 0)
     * - Formata a data de "yyyy-MM-dd_HH-mm-ss" para "yyyy-MM-dd HH:mm:ss"
     * - Mantém exatamente as mesmas colunas do RAW (sem adicionar nada)
     */

    private static void limparDadosParaTrusted(String entrada, String saida) {
        // try-with-resources: fecha arquivos automaticamente (mesmo com erro)
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(entrada))); // lê linha a linha
                PrintWriter gravador = new PrintWriter(new FileWriter(saida))         // grava o CSV de saída
        ) {
            Boolean primeiraLinha = true; // indica a primeira linha (cabeçalho do CSV original)
            String cabecalho = "timestamp,cpu,ram,disco,qtd_processos,mac";
            gravador.println(cabecalho);

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();

                // pula o cabeçalho original do CSV bruto
                if (primeiraLinha) { primeiraLinha = false; continue; }

                // ignora linhas vazias
                if (linha == null || linha.trim().isEmpty()) continue;

                // ORDEM DAS COLUNAS (RAW): [0]=timestamp, [1]=cpu, [2]=ram, [3]=disco, [4]=qtd_processos, [5]=mac
                String[] colunas = linha.replace(';', ',').split(",", -1); //  O "-1" garante que colunas vazias no fim da linha não sejam ignoradas
                if (colunas.length < 6) continue; // linha incompleta? pula

                // leitura (nulos, vírgulas, vazios)
                String  ts    = textoLimpo(colunas[0]);
                Double  cpu   = converterDouble(normalizarNumero(colunas[1]));
                Double  ram   = converterDouble(normalizarNumero(colunas[2]));
                Double  disco = converterDouble(normalizarNumero(colunas[3]));
                Integer qtd   = converterInteiro(colunas[4]);
                String  mac   = textoLimpo(colunas[5]);

                // formata a data do padrão
                String tsFmt = formatarData(ts);

                // grava 1:1 apenas padronizado
                gravador.println(tsFmt + "," + cpu + "," + ram + "," + disco + "," + qtd + "," + mac);
            }

        } catch (Exception e) {
            // Qualquer erro de I/O: não derruba o ETL; apenas avisa
            System.out.println("Erro (Dados TRUSTED): " + e.getMessage());
        }
    }

    /**
     * Lê Processos.csv bruto e grava Processos_Trusted.csv:
     * - Troca ';' por ','
     * - Normaliza números
     * - Formata data
     * - Remove vírgulas internas do nome do processo (para não “quebrar” o CSV)
     * - Mantém exatamente as colunas do RAW
     */

    private static void limparProcessosParaTrusted(String entrada, String saida) {
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(entrada)));
                PrintWriter gravador = new PrintWriter(new FileWriter(saida))
        ) {
            Boolean primeiraLinha = true;
            String cabecalho = "timestamp,processo,cpu,ram,disco,mac";
            gravador.println(cabecalho);

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeiraLinha) { primeiraLinha = false; continue; }
                if (linha == null || linha.trim().isEmpty()) continue;

                // ORDEM DAS COLUNAS (RAW): [0]=timestamp, [1]=processo, [2]=cpu, [3]=ram, [4]=disco, [5]=mac
                String[] colunas = linha.replace(';', ',').split(",", -1); //  O "-1" garante que colunas vazias no fim da linha não sejam ignoradas
                if (colunas.length < 6) continue; // linha incompleta? pula

                String  ts    = textoLimpo(colunas[0]);
                String  proc  = textoLimpo(colunas[1]).replace(",", " ");  // evita separar colunas no CSV final
                Double  cpu   = converterDouble(normalizarNumero(colunas[2]));
                Double  ram   = converterDouble(normalizarNumero(colunas[3]));
                Double  disco = converterDouble(normalizarNumero(colunas[4]));
                String  mac   = textoLimpo(colunas[5]);

                String tsFmt = formatarData(ts);
                gravador.println(tsFmt + "," + proc + "," + cpu + "," + ram + "," + disco + "," + mac);
            }

        } catch (Exception e) {
            System.out.println("Erro (Processos TRUSTED): " + e.getMessage());
        }
    }

    // 6) TRUSTED -> CLIENT (regras de negócio e formatações)

    /**
     * Lê Dados_Trusted.csv e gera:
     * - Dados_Client.csv (na pasta client)
     * - Dados_Client.csv (cópia na raiz)
     *
     * Aqui aplicamos as REGRAS de negócio:
     * - comparamos CPU/RAM/Disco com limites e geramos indicadores (SIM/NAO)
     * - somamos quantos limites estouraram para classificar nível: OK/ATENCAO/PERIGOSO/MUITO_PERIGOSO
     * - formatamos CPU/RAM/Disco como "xx.x%"
     * - atualizamos contadores e "conjuntos" de totens
     */

    private static void tratarDadosSistema(String entrada, String saidaPasta, String saidaRaiz) {
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(entrada)));
                PrintWriter arquivoSaidaPasta = new PrintWriter(new FileWriter(saidaPasta));
                PrintWriter arquivoSaidaRaiz  = new PrintWriter(new FileWriter(saidaRaiz))
        ) {
            Boolean primeiraLinha = true;
            String cabecalho = "timestamp,mac,cpu,ram,disco,qtd_processos,alerta_cpu,alerta_ram,alerta_disco,nivel";
            arquivoSaidaPasta.println(cabecalho);
            arquivoSaidaRaiz.println(cabecalho);

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeiraLinha) { primeiraLinha = false; continue; }
                if (linha.trim().isEmpty()) continue;

                // ORDEM (TRUSTED): [0]=timestamp, [1]=cpu, [2]=ram, [3]=disco, [4]=qtd_processos, [5]=mac
                String[] colunas = linha.split(",", -1);
                if (colunas.length < 6) continue;

                String  ts    = textoLimpo(colunas[0]);
                Double  cpu   = converterDouble(colunas[1]);
                Double  ram   = converterDouble(colunas[2]);
                Double  disco = converterDouble(colunas[3]);
                Integer qtd   = converterInteiro(colunas[4]);
                String  mac   = textoLimpo(colunas[5]);

                // Adiciona o totem na lista SEM duplicar
                if (!totensSistema.contains(mac)) totensSistema.add(mac);
                qtdLeiturasSistema++;

                // Indicadores de alerta por componente
                Boolean alertaCpu   = cpu   >= LIMITE_CPU_SISTEMA;
                Boolean alertaRam   = ram   >= LIMITE_RAM_SISTEMA;
                Boolean alertaDisco = disco >= LIMITE_DISCO_SISTEMA;

                // Classificação por quantidade de alertas
                String nivel = classificarNivel(alertaCpu, alertaRam, alertaDisco);
                if (nivel.equals("OK")) qtdOk++;
                else if (nivel.equals("ATENCAO")) qtdAtencao++;
                else if (nivel.equals("PERIGOSO")) qtdPerigoso++;
                else qtdMuitoPerigoso++;

                // Ex.: formata CPU/RAM/Disco como "xx.x%" e escreve a linha final no CSV
                //     - Exemplo de percentual: 41.0%
                //     - Exemplo de indicador: "SIM" / "NAO"
                String linhaFormatada = ts + "," + mac + "," +
                        formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                        qtd + "," +
                        (alertaCpu ? "SIM" : "NAO") + "," +
                        (alertaRam ? "SIM" : "NAO") + "," +
                        (alertaDisco ? "SIM" : "NAO") + "," +
                        nivel;

                // Grava a mesma linha: 1) na pasta client  2) na raiz do projeto
                arquivoSaidaPasta.println(linhaFormatada);
                arquivoSaidaRaiz.println(linhaFormatada);
            }

        } catch (Exception e) {
            System.out.println("Erro (CLIENT Dados): " + e.getMessage());
        }
    }

    /**
     * Lê Processos_Trusted.csv e gera:
     * - Processos_Client.csv (na pasta client)
     * - Processos_Client.csv (cópia na raiz)
     *
     * Regras por PROCESSO:
     * - indicador de desempenho: estourou CPU OU RAM OU DISCO
     * - suspeito: nome contém um dos termos definidos
     * - risco duplo: alto consumo E suspeito
     * - evitamos duplicar MAC nas listas de totens por categoria
     */

    private static void tratarProcessos(String entrada, String saidaPasta, String saidaRaiz) {
        try (
                Scanner leitor = new Scanner(new FileInputStream(new File(entrada)));
                PrintWriter arquivoSaidaPasta = new PrintWriter(new FileWriter(saidaPasta));
                PrintWriter arquivoSaidaRaiz  = new PrintWriter(new FileWriter(saidaRaiz))
        ) {
            Boolean primeiraLinha = true;
            String cabecalho = "timestamp,mac,processo,cpu,ram,disco,alerta_processo,motivo,tipo_alerta";
            arquivoSaidaPasta.println(cabecalho);
            arquivoSaidaRaiz.println(cabecalho);

            while (leitor.hasNextLine()) {
                String linha = leitor.nextLine();
                if (primeiraLinha) { primeiraLinha = false; continue; }
                if (linha.trim().isEmpty()) continue;

                // ORDEM (TRUSTED): [0]=timestamp, [1]=processo, [2]=cpu, [3]=ram, [4]=disco, [5]=mac
                String[] colunas = linha.split(",", -1);
                if (colunas.length < 6) continue;

                String  ts    = textoLimpo(colunas[0]);
                String  proc  = textoLimpo(colunas[1]);
                Double  cpu   = converterDouble(colunas[2]);
                Double  ram   = converterDouble(colunas[3]);
                Double  disco = converterDouble(colunas[4]);
                String  mac   = textoLimpo(colunas[5]);

                // Totens com processos
                if (!totensProcessos.contains(mac)) totensProcessos.add(mac);
                qtdLinhasProcesso++;

                // Indicadores por processo
                Boolean alertaCpu = cpu   >= LIMITE_CPU_PROCESSO;
                Boolean alertaRam = ram   >= LIMITE_RAM_PROCESSO;
                Boolean alertaDisco = disco >= LIMITE_DISCO_PROCESSO;

                // Alto consumo se QUALQUER um estourou
                Boolean altoConsumo = (alertaCpu || alertaRam || alertaDisco);

                // Marca como suspeito se o nome do processo tiver algum termo da lista
                Boolean suspeito = processoSuspeito(proc);

                // Contadores gerais
                if (altoConsumo) qtdAltoConsumo++;
                if (suspeito) qtdSuspeitos++;
                if (altoConsumo && suspeito) qtdRiscoDuplo++;

                // “Conjuntos” por categoria (evita duplicatas com !contains)
                if (suspeito && !totensComSuspeito.contains(mac)) totensComSuspeito.add(mac);
                if (altoConsumo && !totensComAltoConsumo.contains(mac)) totensComAltoConsumo.add(mac);
                if (altoConsumo && suspeito && !totensComRiscoDuplo.contains(mac)) totensComRiscoDuplo.add(mac);

                // Tipo de alerta (texto final)
                String tipoAlerta = definirTipoAlerta(altoConsumo, suspeito);

                // Motivo (texto final)
                String motivo = "-";
                if (suspeito && altoConsumo) motivo = "PROCESSO_SUSPEITO|ALTO_CONSUMO";
                else if (suspeito) motivo = "PROCESSO_SUSPEITO";
                else if (altoConsumo) motivo = "ALTO_CONSUMO";

                // Ex.: formata CPU/RAM/Disco como "xx.x%" e escreve a linha final no CSV
                //     - Exemplo de percentual: 57.2%
                //     - Exemplo de indicador: "SIM" / "NAO"
                String linhaFormatada = ts + "," + mac + "," + proc + "," +
                        formatarPct(cpu) + "," + formatarPct(ram) + "," + formatarPct(disco) + "," +
                        ((altoConsumo || suspeito) ? "SIM" : "NAO") + "," +
                        motivo + "," + tipoAlerta;

                // Grava a mesma linha: 1) na pasta client  2) na raiz do projeto
                arquivoSaidaPasta.println(linhaFormatada);
                arquivoSaidaRaiz.println(linhaFormatada);
            }

        } catch (Exception e) {
            System.out.println("Erro (CLIENT Processos): " + e.getMessage());
        }
    }

    // 7) HELPERS


    /**
     * Escolhe o caminho do arquivo RAW:
     * - tenta resources;
     * - se não existir, tenta raiz;
     * - se ambos não existirem, avisa e devolve o resource (para não quebrar).
     */

    private static String escolherCaminho(String caminhoResources, String caminhoRaiz) {
        File arquivoResources = new File(caminhoResources);
        if (arquivoResources.exists()) return caminhoResources;

        File arquivoRaiz = new File(caminhoRaiz);
        if (arquivoRaiz.exists()) return caminhoRaiz;

        System.out.println("AVISO: Arquivo não encontrado: " + caminhoResources + " nem " + caminhoRaiz);
        return caminhoResources;
    }

    /**
     * Classifica o "nível" do sistema conforme QUANTOS limites estouraram:
     * - 0 → OK
     * - 1 → ATENCAO
     * - 2 → PERIGOSO
     * - 3 → MUITO_PERIGOSO
     */

    private static String classificarNivel(Boolean alertaCpu, Boolean alertaRam, Boolean alertaDisco) {
        Integer qtdAlertas = 0;

        if (Boolean.TRUE.equals(alertaCpu))   qtdAlertas++;
        if (Boolean.TRUE.equals(alertaRam))   qtdAlertas++;
        if (Boolean.TRUE.equals(alertaDisco)) qtdAlertas++;

        if (qtdAlertas.equals(0)) return "OK";
        if (qtdAlertas.equals(1)) return "ATENCAO";
        if (qtdAlertas.equals(2)) return "PERIGOSO";
        return "MUITO_PERIGOSO";
    }

    /**
     * Decide o “tipo de alerta” do processo (texto final no CSV):
     * - AMBOS      → alto consumo + suspeito
     * - PROCESSO_SUSPEITO
     * - DESEMPENHO
     * - "-" (sem alerta)
     */

    private static String definirTipoAlerta(Boolean altoConsumo, Boolean suspeito) {
        if (Boolean.TRUE.equals(altoConsumo) && Boolean.TRUE.equals(suspeito)) return "AMBOS";
        if (Boolean.TRUE.equals(suspeito)) return "PROCESSO_SUSPEITO";
        if (Boolean.TRUE.equals(altoConsumo)) return "DESEMPENHO";
        return "-";
    }

    /**
     * Verifica se o nome do processo contém algum termo “suspeito”
     */

    private static Boolean processoSuspeito(String nome) {
        if (nome == null) return Boolean.FALSE;
        String n = nome.toLowerCase();
        for (String termo : PROCESSOS_SUSPEITOS) {
            if (n.contains(termo.toLowerCase())) return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    // “Higieniza” texto nulo/vazio
    private static String textoLimpo(String s) {
        return (s == null) ? "" : s.trim();
    }

    // Troca vírgula por ponto e trata vazio como “0” (ex.: "12,5" -> "12.5"; "" -> "0")
    private static String normalizarNumero(String texto) {
        if (texto == null) return "0";
        String numeroTratado = texto.trim();
        return numeroTratado.isEmpty() ? "0" : numeroTratado.replace(",", ".");
    }

    // Converte "2025-10-13_20-30-00" -> "2025-10-13 20:30:00"
    // Se falhar o parse, devolve o original (não quebra)
    private static String formatarData(String txt) {
        try {
            LocalDateTime dt = LocalDateTime.parse(txt, FORMATO_ENTRADA);
            return dt.format(FORMATO_SAIDA);
        } catch (Exception e) {
            return txt;
        }
    }

    // Conversões (nulos/vazios viram zero)
    private static Double converterDouble(String texto) {
        try { return Double.valueOf(texto.trim()); } catch (Exception e) { return 0.0; }
    }
    private static Integer converterInteiro(String texto) {
        try { return Integer.valueOf(texto.trim()); } catch (Exception e) { return 0; }
    }

    // Ex.: 12.34 -> "12.3%"
    private static String formatarPct(Double n) {
        return (Math.round(n * 10.0) / 10.0) + "%";
    }

    // Texto de porcentagem no resumo do console (ex.: " (33.3% do total)")
    private static String percentualTexto(Integer parte, Integer total, String sufixo) {
        if (total.equals(0)) return " (n/a)";
        Double p = (parte * 100.0) / total;
        return " (" + String.format("%.1f", p) + "% " + sufixo + ")";
    }

    // 8) EXECUÇÃO

    public static void main(String[] args) {

        // Decide de onde ler os RAW (resources ou raiz), sem mudar código entre máquinas
        String caminhoDadosRaw = escolherCaminho(CAMINHO_DADOS_RESOURCE, CAMINHO_DADOS_RAIZ);
        String caminhoProcRaw  = escolherCaminho(CAMINHO_PROCESSOS_RESOURCE, CAMINHO_PROCESSOS_RAIZ);

        // UX de progresso
        System.out.println("[1/5] Limpando RAW -> TRUSTED (Dados)...");
        limparDadosParaTrusted(caminhoDadosRaw, SAIDA_DADOS_TRUSTED);

        System.out.println("[2/5] Limpando RAW -> TRUSTED (Processos)...");
        limparProcessosParaTrusted(caminhoProcRaw, SAIDA_PROCESSOS_TRUSTED);

        System.out.println("[3/5] Gerando CLIENT (Sistema)...");
        tratarDadosSistema(SAIDA_DADOS_TRUSTED, SAIDA_DADOS_CLIENT_PASTA, SAIDA_DADOS_CLIENT_RAIZ);

        System.out.println("[4/5] Gerando CLIENT (Processos)...");
        tratarProcessos(SAIDA_PROCESSOS_TRUSTED, SAIDA_PROCESSOS_CLIENT_PASTA, SAIDA_PROCESSOS_CLIENT_RAIZ);

        System.out.println("[5/5] Finalizando resumo...");
        imprimirResumoConsole();
    }

    // Resumo final no console
    private static void imprimirResumoConsole() {
        System.out.println("====================================================");
        System.out.println("                     RESUMO ETL                     ");
        System.out.println("====================================================");
        System.out.println("Totens distintos (Sistema): " + totensSistema.size());
        System.out.println("Leituras de sistema: " + qtdLeiturasSistema);
        System.out.println("----------------------------------------------------");
        System.out.println("Status das leituras:");
        System.out.println("OK: " + qtdOk + " | Atenção: " + qtdAtencao +
                " | Perigoso: " + qtdPerigoso +
                " | Muito Perigoso: " + qtdMuitoPerigoso);
        System.out.println("----------------------------------------------------");
        System.out.println("Linhas de processos: " + qtdLinhasProcesso);
        System.out.println("Com sobrecarga: " + qtdAltoConsumo + percentualTexto(qtdAltoConsumo, qtdLinhasProcesso, "do total"));
        System.out.println("Suspeitos: " + qtdSuspeitos + percentualTexto(qtdSuspeitos, qtdLinhasProcesso, "do total"));
        System.out.println("Risco duplo: " + qtdRiscoDuplo + percentualTexto(qtdRiscoDuplo, qtdLinhasProcesso, "do total"));
        System.out.println("----------------------------------------------------");
        System.out.println("Totens (processos): " + totensProcessos.size());
        System.out.println("Totens com ≥1 suspeito: " + totensComSuspeito.size() +
                percentualTexto(totensComSuspeito.size(), totensProcessos.size(), "dos totens com processos"));
        System.out.println("Totens com ≥1 alto consumo: " + totensComAltoConsumo.size() +
                percentualTexto(totensComAltoConsumo.size(), totensProcessos.size(), "dos totens com processos"));
        System.out.println("Totens com risco duplo: " + totensComRiscoDuplo.size() +
                percentualTexto(totensComRiscoDuplo.size(), totensProcessos.size(), "dos totens com processos"));
        System.out.println("====================================================");
        System.out.println("Explicação S3 (simulada) — caminhos reais:");
        System.out.println(" RAW:      " + CAMINHO_DADOS_RESOURCE + " | " + CAMINHO_PROCESSOS_RESOURCE + " (ou raiz)");
        System.out.println(" TRUSTED:  " + SAIDA_DADOS_TRUSTED + " | " + SAIDA_PROCESSOS_TRUSTED);
        System.out.println(" CLIENT:   " + SAIDA_DADOS_CLIENT_PASTA + " | " + SAIDA_PROCESSOS_CLIENT_PASTA);
        System.out.println(" CLIENT 2: " + SAIDA_DADOS_CLIENT_RAIZ + " | " + SAIDA_PROCESSOS_CLIENT_RAIZ + "  (cópias na raiz)");
        System.out.println("====================================================");
    }
}