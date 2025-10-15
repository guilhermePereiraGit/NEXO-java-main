package school.sptech;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

    static Double LIMITE_CPU_SISTEMA   = 6.5;   // %
    static Double LIMITE_RAM_SISTEMA   = 82.0;  // %
    static Double LIMITE_DISCO_SISTEMA = 39.0;  // %

    static Double LIMITE_CPU_PROCESSO   = 1.0;   // %
    static Double LIMITE_RAM_PROCESSO   = 5.0;   // %
    static Double LIMITE_DISCO_PROCESSO = 300.0; // MB escritos

    static String[] PROCESSOS_SUSPEITOS = {
            "MemCompression", "Discord.exe", "Code.exe"
    };

    // 2) CAMINHOS (RAW/SAÍDAS)

    // Onde procuramos os CSVs brutos (RAW): primeiro resources, depois raiz
    static String CAMINHO_DADOS_RESOURCE     = "src/main/resources/Dados.csv";
    static String CAMINHO_DADOS_RAIZ         = "Dados.csv";
    static String CAMINHO_PROCESSOS_RESOURCE = "src/main/resources/Processos.csv";
    static String CAMINHO_PROCESSOS_RAIZ     = "Processos.csv";

    // Pastas “simulando” buckets S3
    static String PASTA_TRUSTED = "storage/trusted";
    static String PASTA_CLIENT  = "storage/client";

    // Saídas TRUSTED
    static String SAIDA_DADOS_TRUSTED     = PASTA_TRUSTED + "/Dados_Trusted.csv";
    static String SAIDA_PROCESSOS_TRUSTED = PASTA_TRUSTED + "/Processos_Trusted.csv";

    // Saídas CLIENT
    static String SAIDA_DADOS_CLIENT_PASTA     = PASTA_CLIENT + "/Dados_Client.csv";
    static String SAIDA_PROCESSOS_CLIENT_PASTA = PASTA_CLIENT + "/Processos_Client.csv";
    static String SAIDA_DADOS_CLIENT_RAIZ      = "Dados_Client.csv";
    static String SAIDA_PROCESSOS_CLIENT_RAIZ  = "Processos_Client.csv";

    // 3) FORMATO DE DATA — Python usa underline

    static DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    static DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    // 4) CONTADORES e “conjuntos” (sem duplicatas)

    // Contadores gerais do SISTEMA (linhas de Dados.csv tratadas):
    static Integer qtdLeiturasSistema = 0;
    static Integer qtdOk = 0;
    static Integer qtdAtencao = 0;
    static Integer qtdPerigoso = 0;
    static Integer qtdMuitoPerigoso = 0;

    // Contadores gerais de PROCESSOS:
    static Integer qtdLinhasProcesso = 0;
    static Integer qtdAltoConsumo = 0;
    static Integer qtdSuspeitos = 0;
    static Integer qtdRiscoDuplo = 0;

    // “Conjuntos” de totens (sem duplicar MAC) — ArrayList + contains()
    static ArrayList<String> totensSistema        = new ArrayList<>();
    static ArrayList<String> totensProcessos      = new ArrayList<>();
    static ArrayList<String> totensComSuspeito    = new ArrayList<>();
    static ArrayList<String> totensComAltoConsumo = new ArrayList<>();
    static ArrayList<String> totensComRiscoDuplo  = new ArrayList<>();

    // 5) RAW -> TRUSTED

    /**
     * Lê Dados.csv bruto e grava Dados_Trusted.csv:
     * - Troca ';' por ','
     * - Normaliza números (troca vírgula por ponto; vazio -> 0)
     * - Formata a data de "yyyy-MM-dd_HH-mm-ss" para "yyyy-MM-dd HH:mm:ss"
     * - Mantém exatamente as mesmas colunas do RAW (sem adicionar nada)
     */

    private static void limparDadosParaTrusted(String nomeArqOrigem, String nomeArqDestino) {
        FileReader arqLeitura = null;
        Scanner entrada = null;
        OutputStreamWriter saida = null;
        Boolean deuRuim = false;
        nomeArqOrigem += ".csv";
        nomeArqDestino += ".csv";

        try {
            arqLeitura = new FileReader(nomeArqOrigem);
            entrada = new Scanner(arqLeitura);

            saida = new OutputStreamWriter(new FileOutputStream(nomeArqDestino), StandardCharsets.UTF_8);
        } catch (FileNotFoundException erro) {
            System.out.println("Arquivo de origem inexistente!");
            deuRuim = true;
        }

        try {
            Boolean cabecalho = true;

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",");

                if (cabecalho) {
                    saida.write(linha + "\n");

                    cabecalho = false;

                } else {
                    while (valores.length < 6) {
                        linha += ",";
                        valores = linha.split(",");
                    }

                    String ts     = textoLimpo(valores[0]);
                    Double cpu    = converterDouble(normalizarNumero(textoLimpo(valores[1])));
                    Double ram    = converterDouble(normalizarNumero(textoLimpo(valores[2])));
                    Double disco  = converterDouble(normalizarNumero(textoLimpo(valores[3])));
                    Integer procs = converterInteiro(textoLimpo(valores[4]));
                    String mac    = textoLimpo(valores[5]);

                    String tsFmt = formatarData(ts);

                    saida.write(tsFmt + "," + cpu + "," + ram + "," + disco + "," + procs + "," + mac + "\n");
                }
            }
        } catch (IOException erro) {
            System.out.println("Erro ao ler ou gravar o arquivo!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            try {
                if (entrada != null) entrada.close();
                if (arqLeitura != null) arqLeitura.close();
                if (saida != null) saida.close();
            } catch (IOException erro) {
                System.out.println("Erro ao fechar o arquivo!");
                deuRuim = true;
            }

            if (deuRuim) {
                System.exit(1);
            }
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

    private static void limparProcessosParaTrusted(String nomeArqOrigem, String nomeArqDestino) {
        FileReader arqLeitura = null;
        Scanner entrada = null;
        OutputStreamWriter saida = null;
        Boolean deuRuim = false;
        nomeArqOrigem += ".csv";
        nomeArqDestino += ".csv";

        try {
            arqLeitura = new FileReader(nomeArqOrigem);
            entrada = new Scanner(arqLeitura);

            saida = new OutputStreamWriter(new FileOutputStream(nomeArqDestino), StandardCharsets.UTF_8);
        } catch (FileNotFoundException erro) {
            System.out.println("Arquivo de origem inexistente!");
            deuRuim = true;
        }

        try {
            Boolean cabecalho = true;

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(";");

                if (cabecalho) {
                    saida.write(linha + "\n");

                    System.out.println();
                    cabecalho = false;
                } else {
                    for (String valor : valores) {
                        String  ts    = textoLimpo(valores[0]);
                        String  proc  = textoLimpo(valores[1]).replace(",", " ");
                        Double  cpu   = converterDouble(normalizarNumero(valores[2]));
                        Double  ram   = converterDouble(normalizarNumero(valores[3]));
                        Double  disco = converterDouble(normalizarNumero(valores[4]));
                        String  mac   = textoLimpo(valores[5]);

                        String tsFmt = formatarData(ts);

                        saida.write(tsFmt + ";" + proc + ";" + cpu + ";" + ram + ";" + disco + ";" + mac);
                    }
                    System.out.println();
                }
            }
        } catch (IOException erro) {
            System.out.println("Erro ao ler ou gravar o arquivo!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            try {
                if (entrada != null) entrada.close();
                if (arqLeitura != null) arqLeitura.close();
                if (saida != null) saida.close();
            } catch (IOException erro) {
                System.out.println("Erro ao fechar o arquivo!");
                deuRuim = true;
            }

            if (deuRuim) {
                System.exit(1);
            }
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
                adicionarUnico(totensSistema, mac);
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
     * Verifica se já existe aquele valor dentro do array antes de adiciona-lo:
     */

    private static void adicionarUnico(ArrayList<String> lista, String valor) {
        for (String item : lista) {
            if (item.equals(valor)) {
                return;
            }
        }
        lista.add(valor);
    }


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


    private static String textoLimpo(String s) {
        if (s == null || s.trim().isEmpty()) {
            return "Dado_perdido";
        }
        s = s.trim();
        return s;
    }

    private static String normalizarNumero(String texto) {
        if (texto == null) {
            return "Dado_perdido";
        }
        String numeroTratado = texto.trim();

        if (numeroTratado.isEmpty()) {
            return "Dado_perdido";
        }
        numeroTratado = numeroTratado.replace(",", ".");
        return numeroTratado;
    }

    // Converte "2025-10-13_20-30-00" -> "2025-10-13 20:30:00"
    // Se falhar o parse, devolve o original (não quebra)
    private static String formatarData(String txt) {
        try {
            if (txt == null || txt.trim().isEmpty() || txt.equalsIgnoreCase("Dado_perdido")) {
                return "Dado_perdido";
            }
            txt = txt.trim();

            LocalDateTime dt = LocalDateTime.parse(txt, FORMATO_ENTRADA);
            return dt.format(FORMATO_SAIDA);

        } catch (Exception e) {
            return "Dado_perdido";
        }
    }

    // Conversões (nulos/vazios viram zero)
    private static Double converterDouble(String texto) {
        if (texto == null || texto.trim().isEmpty()) {
            return 0.0;
        }
        try {
            return Double.valueOf(texto.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }
    private static Integer converterInteiro(String texto) {
        if (texto == null || texto.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.valueOf(texto.trim());
        } catch (Exception e) {
            return 0;
        }
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

        // UX de progresso
        System.out.println("[1/5] Limpando RAW -> TRUSTED (Dados)...");
        limparDadosParaTrusted("Dados", "Dados_Trusted");

        System.out.println("[2/5] Limpando RAW -> TRUSTED (Processos)...");
        // limparProcessosParaTrusted("Processos", "Processos_Trusted");

        System.out.println("[3/5] Gerando CLIENT (Sistema)...");
        // tratarDadosSistema(SAIDA_DADOS_TRUSTED, SAIDA_DADOS_CLIENT_PASTA, SAIDA_DADOS_CLIENT_RAIZ);

        System.out.println("[4/5] Gerando CLIENT (Processos)...");
        // tratarProcessos(SAIDA_PROCESSOS_TRUSTED, SAIDA_PROCESSOS_CLIENT_PASTA, SAIDA_PROCESSOS_CLIENT_RAIZ);

        System.out.println("[5/5] Finalizando resumo...");
        // imprimirResumoConsole();
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