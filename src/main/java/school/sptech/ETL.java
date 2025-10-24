package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import io.github.cdimascio.dotenv.Dotenv;

import static school.sptech.IntegracaoJira.criarChamado;
import static school.sptech.NotificadorSlack.enviarMensagem;

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

    // LIMITES / PARÂMETROS
    static Integer LIMITE_QTD_PROCESSOS = 341;

    static Double LIMITE_CPU_PROCESSO   = 1.0;   // %
    static Double LIMITE_RAM_PROCESSO   = 5.0;   // %
    static Double LIMITE_DISCO_PROCESSO = 300.0; // MB escritos

    static String[] PROCESSOS_SUSPEITOS = {
            "MemCompression", "Discord.exe", "Code.exe"
    };

    // FORMATO DE DATA — Python usa underline

    static DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    static DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // RAW -> TRUSTED

    /**
     * Lê Dados.csv bruto e grava Dados_Trusted.csv:
     * - Troca ';' por ','
     * - Normaliza números (troca vírgula por ponto; vazio -> 0)
     * - Formata a data de "yyyy-MM-dd_HH-mm-ss" para "yyyy-MM-dd HH:mm:ss"
     * - Mantém exatamente as mesmas colunas do RAW (sem adicionar nada)
     */

    static Dotenv dotenv = Dotenv.load();
    private static String BUCKET_RAW = dotenv.get("BUCKET_RAW");
    private static String BUCKET_TRUSTED = dotenv.get("BUCKET_TRUSTED");
    private static Region S3_REGION = Region.US_EAST_1;
    private static S3Client s3Client;
    static {
        try {
            dotenv = Dotenv.load();

            String accessKey = dotenv.get("AWS_ACCESS_KEY_ID").trim();
            String secretKey = dotenv.get("AWS_SECRET_ACCESS_KEY").trim();
            String sessionToken = dotenv.get("AWS_SESSION_TOKEN").trim();

            AwsSessionCredentials sessionCreds = AwsSessionCredentials.create(
                    accessKey,
                    secretKey,
                    sessionToken
            );

            s3Client = S3Client.builder()
                    .region(S3_REGION)
                    .credentialsProvider(StaticCredentialsProvider.create(sessionCreds))
                    .build();

        } catch (Exception e) {
            System.out.println("Erro ao criar cliente S3 com credenciais temporárias!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    private static void limparDadosParaTrusted(String macOrigem) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();

        try {
            // Tentativa de ler arquivo no S3
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key("registros/" + macOrigem + "/dados.csv")
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Map<String, List<String>> linhasPorMac = new HashMap<>();
            String headerLine = null;

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);

                if (cabecalho) {
                    headerLine = linha;
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];
                    for (int i = 0; i < numeroColunasEsperadas; i++) {
                        if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty()) {
                            valoresCompletos[i] = valores[i];
                        } else {
                            valoresCompletos[i] = "dado_perdido";
                        }
                    }

                    String ts     = textoLimpo(valoresCompletos[0]);
                    String cpu    = normalizarNumero(textoLimpo(valoresCompletos[1]));
                    String ram    = normalizarNumero(textoLimpo(valoresCompletos[2]));
                    String disco  = normalizarNumero(textoLimpo(valoresCompletos[3]));
                    String procs  = textoLimpo(valoresCompletos[4]);
                    String mac    = textoLimpo(valoresCompletos[5]);
                    String tsFmt  = formatarData(ts);

                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + procs + "," + mac;

                    linhasPorMac.computeIfAbsent(mac, k -> new ArrayList<>()).add(linhaProcessada);
                }
            }

            // Para cada mac, faz merge e upload
            for (Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = entry.getKey();
                List<String> novasLinhas = entry.getValue();
                String objetoTrustedKey = "registros/" + mac + "/dados.csv";
                mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
            }

        } catch (NoSuchKeyException e) {
            // Arquivo RAW não existe então não dá para processar
            System.out.println("Arquivo RAW não existe para o MAC " + macOrigem + ": " + e.getMessage());
            deuRuim = true;
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) entrada.close();
            if (deuRuim) System.exit(1);
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

    private static void limparProcessosParaTrusted(String macOrigem) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key("registros/" + macOrigem + "/processos.csv")
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Map<String, List<String>> linhasPorMac = new HashMap<>();
            String headerLine = null;

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);

                if (cabecalho) {
                    headerLine = linha;
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];
                    for (int i = 0; i < numeroColunasEsperadas; i++) {
                        if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty()) {
                            valoresCompletos[i] = valores[i];
                        } else {
                            valoresCompletos[i] = "dado_perdido";
                        }
                    }

                    String ts     = textoLimpo(valoresCompletos[0]);
                    String cpu    = normalizarNumero(textoLimpo(valoresCompletos[1]));
                    String ram    = normalizarNumero(textoLimpo(valoresCompletos[2]));
                    String disco  = normalizarNumero(textoLimpo(valoresCompletos[3]));
                    String nomeProc = textoLimpo(valoresCompletos[4]).replace(",", " ");
                    String mac    = textoLimpo(valoresCompletos[5]);
                    String tsFmt  = formatarData(ts);

                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + nomeProc + "," + mac;
                    linhasPorMac.computeIfAbsent(mac, k -> new ArrayList<>()).add(linhaProcessada);
                }
            }

            for (Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = entry.getKey();
                List<String> novasLinhas = entry.getValue();
                String objetoTrustedKey = "registros/" + mac + "/processos.csv";
                mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
            }

        } catch (NoSuchKeyException e) {
            System.out.println("Arquivo RAW de processos não existe para MAC " + macOrigem + ". Ignorando.");
            // Não falha; S3 merge vai criar arquivo se houver linhas novas
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) entrada.close();
            if (deuRuim) System.exit(1);
        }
    }



    // TRUSTED -> CLIENT (regras de negócio e formatações)

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

    private static void taxaAlertas(String nomeDirOrigem, String nomeDirDestino, Totem totem) {
        FileReader arqLeitura = null;
        Scanner entrada = null;
        OutputStreamWriter saida = null;
        Boolean deuRuim = false;
        nomeDirOrigem += ".csv";
        nomeDirDestino += totem.getNumMac() + ".csv";

        try {
            arqLeitura = new FileReader(nomeDirOrigem);
            entrada = new Scanner(arqLeitura);
            saida = new OutputStreamWriter(new FileOutputStream(nomeDirDestino), StandardCharsets.UTF_8);
        } catch (FileNotFoundException erro) {
            System.out.println("Arquivo de origem inexistente!");
            deuRuim = true;
        }

        try {
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Integer qtdAlertas = 0;
            Integer qtdLinhas = 0;
            Boolean pular = false;


            Integer limiteCPU = null;
            Integer limiteRAM = null;
            Integer limiteDisco = null;
            for (Parametro p : totem.getModelo().getParametros()) {
                String parametroLowerCase = p.getTipoParametro().getNome().toLowerCase();
                if(p.getLimite() != null) {
                    if (parametroLowerCase.contains("cpu")) {
                        limiteCPU = p.getLimite();
                    }
                    if (parametroLowerCase.contains("ram")) {
                        limiteRAM = p.getLimite();
                    }
                    if (parametroLowerCase.contains("disco")) {
                        limiteDisco = p.getLimite();
                    }
                }
            }

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);

                if (cabecalho) {
                    saida.write("timestamp" + "," + "mac" + "," + "alertaJira" + "," + "cpu" + "," + "ram" + "," + "disco" + "," + "qtdProcessos" + "\n");
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];

                    for (int i = 0; i < numeroColunasEsperadas; i++) {
                        if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty()) {
                            valoresCompletos[i] = valores[i];
                        } else {
                            pular = true;
                        }
                    }

                    if (!pular) {
                        qtdLinhas++;
                        String ts     = textoLimpo(valoresCompletos[0]);
                        Double cpu    = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[1])));
                        Double ram    = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[2])));
                        Double disco  = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[3])));
                        Integer procs = converterInteiro(textoLimpo(valoresCompletos[4]));
                        String mac    = textoLimpo(valoresCompletos[5]);

                        Boolean alertaCpu       = cpu   >= limiteCPU;
                        Boolean alertaRam       = ram   >= limiteRAM;
                        Boolean alertaDisco     = disco >= limiteDisco;
                        Boolean alertaProcessos = procs >= LIMITE_QTD_PROCESSOS;
                        Boolean alerta = false;
                        String parametrosUltrapassados = "";

                        if (alertaCpu) {
                            qtdAlertas++;
                            parametrosUltrapassados += " CPU, ";
                        }
                        if (alertaRam) {
                            qtdAlertas++;
                            parametrosUltrapassados += " RAM, ";
                        }
                        if (alertaDisco) {
                            qtdAlertas++;
                            parametrosUltrapassados += " Uso de disco, ";
                        }
                        if (alertaProcessos) { qtdAlertas++; }


                        if (qtdAlertas >= 7 && qtdLinhas % 12 == 0) {
                            alerta = true;
                            enviarMensagem("Alerta! Parâmetro(s)" + parametrosUltrapassados + "acima do limite no totem " + totem.getNumMac() + " às " + ts);
                            criarChamado("Totem " + totem.getNumMac() + " acima do limite de segurança", "O totem de MAC " + totem.getNumMac() +
                                    "ultrapassou o(s) limite(s) estabelecidos para seus parâmetros. Parâmetros ultrapassados: " + parametrosUltrapassados);
                            qtdAlertas = 0;
                        }

                        String tsFmt = formatarData(ts);

                        saida.write(tsFmt + "," + mac + "," + alerta + "," + alertaCpu + "," + alertaRam + "," + alertaDisco + "," + alertaProcessos + "\n");
                    }
                    pular = false;
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

    private static void relatorioTotem(String nomeDirOrigem, String nomeDirDestino, Totem totem) {
        FileReader arqLeitura = null;
        Scanner entrada = null;
        OutputStreamWriter saida = null;
        Boolean deuRuim = false;
        nomeDirOrigem += ".csv";
        nomeDirDestino += totem.getNumMac() + ".csv";

        try {
            arqLeitura = new FileReader(nomeDirOrigem);
            entrada = new Scanner(arqLeitura);
            saida = new OutputStreamWriter(new FileOutputStream(nomeDirDestino), StandardCharsets.UTF_8);
        } catch (FileNotFoundException erro) {
            System.out.println("Arquivo de origem inexistente!");
            deuRuim = true;
        }

        try {
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Integer qtdAlertas = 0;
            Integer totalAlertas = 0;
            Integer qtdAtencao = 0;
            Integer qtdPerigo = 0;
            Integer qtdCritico = 0;
            Integer qtdLinhas = 0;
            Double maxCPU = 0.0;
            Double maxRam = 0.0;
            Double maxDisco = 0.0;
            Integer maxProcesosRegistrados = 0;
            Boolean pular = false;
            String ts = "";
            String mac = "";

            Integer limiteCPU = 0;
            Integer limiteRAM = 0;
            Integer limiteDisco = 0;
            for (Parametro p : totem.getModelo().getParametros()) {
                String parametroLowerCase = p.getTipoParametro().getNome().toLowerCase();
                if (parametroLowerCase.contains("cpu")) {
                    limiteCPU = p.getLimite();
                }
                if (parametroLowerCase.contains("ram")) {
                    limiteRAM = p.getLimite();
                }
                if (parametroLowerCase.contains("disco")) {
                    limiteDisco = p.getLimite();
                }
            }

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);

                if (cabecalho) {
                    saida.write("timestamp" + "," + "mac" + "," + "totalAlertas" + "," + "alertasAtencao" + "," +
                            "alertasPerigo" + "," + "alertasCritico" + "," + "maxCpu" + "," +
                            "maxRam" + "," + "maxDisco" + "," + "qtdMaxProcessos" + "\n");

                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];



                    for (int i = 0; i < numeroColunasEsperadas; i++) {
                        if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty()) {
                            valoresCompletos[i] = valores[i];
                        } else {
                            pular = true;
                        }
                    }

                    if (!pular) {
                        qtdLinhas++;
                        ts            = textoLimpo(valoresCompletos[0]);
                        Double cpu    = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[1])));
                        Double ram    = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[2])));
                        Double disco  = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[3])));
                        Integer procs = converterInteiro(textoLimpo(valoresCompletos[4]));
                        mac           = textoLimpo(valoresCompletos[5]);

                        Boolean alertaCpu       = cpu   >= limiteCPU;
                        Boolean alertaRam       = ram   >= limiteRAM;
                        Boolean alertaDisco     = disco >= limiteDisco;
                        Boolean alertaProcessos = procs >= LIMITE_QTD_PROCESSOS;

                        if (alertaCpu) {
                            qtdAlertas++;
                        }
                        if (alertaRam) {
                            qtdAlertas++;
                        }
                        if (alertaDisco) {
                            qtdAlertas++;
                        }
                        if (alertaProcessos) { qtdAlertas++; }

                        if (cpu > maxCPU) {
                            maxCPU = cpu;
                        }
                        if (ram > maxRam) {
                            maxRam = ram;
                        }
                        if (disco > maxDisco) {
                            maxDisco = disco;
                        }
                        if (procs > maxProcesosRegistrados) {
                            maxProcesosRegistrados = procs;
                        }

                    }
                    pular = false;
                }
            }
            String tsFmt = formatarData(ts);
            saida.write(tsFmt + "," + mac + "," + totalAlertas + "," + qtdAtencao + "," +
                    qtdPerigo + "," + qtdCritico + "," + maxCPU + "," + maxRam + "," +
                    maxDisco + "," + maxProcesosRegistrados + "\n");

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


    // HELPERS

    private static String textoLimpo(String s) {
        if (s == null || s.trim().isEmpty()) {
            return "Dado_perdido";
        }
        s = s.trim();
        return s;
    }

    private static String normalizarNumero(String texto) {
        if (texto == null || texto.trim().isEmpty()) {
            return "Dado_perdido";
        }
        String numeroTratado = texto.trim();

        numeroTratado = numeroTratado.replace(",", ".");
        return numeroTratado;
    }
    
    private static String formatarData(String texto) {
        try {
            if (texto == null || texto.trim().isEmpty() || texto.equalsIgnoreCase("Dado_perdido")) {
                return "Dado_perdido";
            }
            texto = texto.trim();

            LocalDateTime dt = LocalDateTime.parse(texto, FORMATO_ENTRADA);
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

    public static List<Totem> carregarTotensComLimites(JdbcTemplate con) {

        List<Totem> totens = con.query("""
                SELECT t.idTotem, t.numMac, t.instalador, t.status_totem AS statusTotem, t.dataInstalacao,
                m.idModelo, m.nome AS modeloNome, m.criador, m.tipo, m.descricao_arquitetura AS descricaoArquitetura,
                m.status AS statusModelo, m.fkEmpresa
                FROM totem t
                INNER JOIN modelo m ON t.fkModelo = m.idModelo
                """, new TotemRowMapper());

        for (Totem totem : totens) {
            Modelo modelo = totem.getModelo();
            if (modelo != null) {
                List<Parametro> parametros = con.query("""
                        SELECT p.idParametro, p.limite, tp.idTipo_Parametro AS idTipoParametro, tp.nome AS nome
                        FROM Parametro p
                        INNER JOIN Tipo_Parametro tp ON p.fkTipoParametro = tp.idTipo_Parametro
                        WHERE p.fkModelo = ?
                        """, new ParametroRowMapper(), modelo.getIdModelo());
                modelo.setParametros(parametros);
            }
        }

        return totens;
    }

    /**
     * Faz merge entre o conteúdo existente no S3 (se houver) e as novas linhas,
     * removendo duplicatas com base em chave timestamp+mac e mantendo a última ocorrência (sem duplicação).
     *
     * @param header linha de cabeçalho original (pode ser null, mas esperamos não ser)
     * @param s3Key  chave do objeto no bucket-trusted (ex.: registros/{mac}/Dados_Trusted.csv)
     * @param novasLinhas lista de linhas (já formatadas CSV) para acrescentar / sobrescrever
     */
    private static void mergeAndUploadToTrustedBucket(String header, String s3Key, List<String> novasLinhas) {
        try {
            Map<String, String> chaveParaLinha = new LinkedHashMap<>();

            // Tenta baixar arquivo existente
            try {
                GetObjectRequest getExisting = GetObjectRequest.builder()
                        .bucket(BUCKET_TRUSTED)
                        .key(s3Key)
                        .build();

                ResponseInputStream<GetObjectResponse> existingStream = s3Client.getObject(getExisting);
                try (Scanner sc = new Scanner(new InputStreamReader(existingStream, StandardCharsets.UTF_8))) {
                    boolean first = true;
                    while (sc.hasNextLine()) {
                        String linhaExistente = sc.nextLine();
                        if (first) { first = false; continue; }
                        String[] cols = linhaExistente.split(",", -1);
                        if (cols.length >= 2) {
                            String key = cols[0].trim() + "__" + cols[cols.length - 1].trim();
                            chaveParaLinha.put(key, linhaExistente);
                        } else {
                            chaveParaLinha.put(UUID.randomUUID().toString(), linhaExistente);
                        }
                    }
                }
            } catch (NoSuchKeyException e) {
                // arquivo não existe: será criado
                System.out.println("Arquivo trusted não existe. Será criado: " + s3Key);
            }

            // Adiciona novas linhas
            for (String nova : novasLinhas) {
                String[] cols = nova.split(",", -1);
                if (cols.length >= 2) {
                    String key = cols[0].trim() + "__" + cols[cols.length - 1].trim();
                    chaveParaLinha.put(key, nova);
                } else {
                    chaveParaLinha.put(UUID.randomUUID().toString(), nova);
                }
            }

            // Monta conteúdo final
            StringBuilder sb = new StringBuilder();
            if (header != null) {
                sb.append(header).append("\n");
            } else {
                sb.append("timestamp,cpu,ram,disco,qtdProcessos,mac\n");
            }
            for (String line : chaveParaLinha.values()) {
                sb.append(line).append("\n");
            }

            // Upload
            // (S3 cria "diretórios" automaticamente, então não precisa criar pasta explicitamente)
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(BUCKET_TRUSTED)
                    .key(s3Key)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putReq, RequestBody.fromString(sb.toString(), StandardCharsets.UTF_8));
            System.out.println("Upload/merge OK: s3://" + BUCKET_TRUSTED + "/" + s3Key);

        } catch (Exception e) {
            System.out.println("Erro durante merge/upload para trusted: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // EXECUÇÃO

    public static void main(String[] args) {
        // Conectando ao banco de dados remoto e selecionando parâmetros
        Connection connection = new Connection();
        JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

        List<Totem> totens = carregarTotensComLimites(con);

        // UX de progresso
        for (Totem totem : totens) {
            limparDadosParaTrusted(totem.getNumMac());
            limparProcessosParaTrusted(totem.getNumMac());
            taxaAlertas("Dados", "alertas", totem);
            relatorioTotem("Dados", "relatórioTotem", totem);
        }
        System.out.println("[1/4] Limpando RAW -> TRUSTED (Dados)...");

        System.out.println("[2/4] Limpando RAW -> TRUSTED (Processos)...");

        System.out.println("[3/4] Gerando Taxa de Alertas...");

        System.out.println("[4/4] Gerando Relatório do Totem (Processos)...");
    }
}