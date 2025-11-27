package school.sptech;

import org.h2.jdbc.JdbcConnection;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
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
public class
ETL {
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

    private static String BUCKET_RAW = "bucket-raw-nexo";
    private static String BUCKET_TRUSTED = "bucket-trusted-nexo";
    private static String BUCKET_CLIENT = "bucket-client-nexo";
    private static Region S3_REGION = Region.US_EAST_1;
    private static S3Client s3Client;
    static {
        try {
            s3Client = S3Client.builder()
                    .region(S3_REGION)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

        } catch (Exception e) {
            System.out.println("Erro ao criar cliente S3 com credenciais automáticas!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }



    private static void limparDadosParaTrusted(String macOrigem, JdbcTemplate con) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();

        try {
            // Tentativa de ler arquivo no S3
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key( buscarIdEmpresaPorMac(con, macOrigem) + "/" + macOrigem + "/" + LocalDate.now(ZoneId.of("America/Sao_Paulo")).toString() + "/dados.csv")
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 9;
            Map<String, Map<String, List<String>>> linhasPorMacEDia = new HashMap<>();
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

                    String ts      = textoLimpo(valoresCompletos[0]);
                    String cpu     = normalizarNumero(textoLimpo(valoresCompletos[1]));
                    String ram     = normalizarNumero(textoLimpo(valoresCompletos[2]));
                    String disco   = normalizarNumero(textoLimpo(valoresCompletos[3]));
                    String procs   = textoLimpo(valoresCompletos[4]);
                    String uptime  = textoLimpo(String.valueOf(Double.parseDouble(valoresCompletos[5]) / 86400));
                    String mac     = textoLimpo(valoresCompletos[6]);
                    String modelo  = textoLimpo(valoresCompletos[7]);
                    String empresa = textoLimpo(valoresCompletos[8]);
                    String tsFmt   = formatarData(ts);

                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + procs + "," + uptime + "," + mac + "," + modelo + "," + empresa;

                    String dia = tsFmt.substring(0, 10);
                    linhasPorMacEDia
                            .computeIfAbsent(mac, k -> new HashMap<>())
                            .computeIfAbsent(dia, k -> new ArrayList<>())
                            .add(linhaProcessada);

                }
            }

            // Para cada mac, faz merge e upload
            for (Map.Entry<String, Map<String, List<String>>> macEntry : linhasPorMacEDia.entrySet()) {
                String mac = macEntry.getKey();
                Integer idEmpresa = buscarIdEmpresaPorMac(con, mac);
                for (Map.Entry<String, List<String>> diaEntry : macEntry.getValue().entrySet()) {
                    String dia = diaEntry.getKey();
                    List<String> novasLinhas = diaEntry.getValue();
                    String objetoTrustedKey = idEmpresa + "/" + mac + "/" + dia + "/dados.csv";
                    mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
                }
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

    private static void limparProcessosParaTrusted(String macOrigem, JdbcTemplate con) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key( buscarIdEmpresaPorMac(con, macOrigem) + "/" + macOrigem + "/" + LocalDate.now(ZoneId.of("America/Sao_Paulo")).toString() + "/dados.csv")
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 8;
            Map<String, List<String>> linhasPorMac = new HashMap<>();
            String headerLine = null;

            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);

                if (cabecalho) {
                    headerLine = linha + ",modelo,idEmpresa";
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
                    String modelo  = textoLimpo(valoresCompletos[6]);
                    String empresa = textoLimpo(valoresCompletos[7]);
                    String tsFmt  = formatarData(ts);

                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + nomeProc + "," + mac + "," + modelo + "," + empresa;
                    linhasPorMac.computeIfAbsent(mac, k -> new ArrayList<>()).add(linhaProcessada);
                }
            }

            for (Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = entry.getKey();
                List<String> novasLinhas = entry.getValue();
                String objetoTrustedKey = buscarIdEmpresaPorMac(con, macOrigem) + "/" + macOrigem + "/" + LocalDate.now(ZoneId.of("America/Sao_Paulo")).toString() + "/processos.csv";
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

    private static void taxaAlertas(Totem totem, JdbcTemplate con) {
        Scanner entrada = null;
        StringBuilder saida = new StringBuilder();
        Boolean deuRuim = false;

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_RAW)
                    .key( buscarIdEmpresaPorMac(con, totem.getNumMac()) + "/" + totem.getNumMac() + "/" + LocalDate.now(ZoneId.of("America/Sao_Paulo")).toString() + "/dados.csv")
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 8;
            Integer qtdAlertas = 0;
            Integer qtdLinhas = 0;
            Boolean pular = false;

            Integer limiteMinCPU = null;
            Integer limiteMaxCPU = null;
            Integer limiteMinRAM = null;
            Integer limiteMaxRAM = null;
            Integer limiteMinDisco = null;
            Integer limiteMaxDisco = null;
            Integer limiteMinProcessos = null;
            Integer limiteMaxProcessos = null;

            for (Parametro p : totem.getModelo().getParametros()) {
                String parametroLowerCase = p.getTipoParametro().getComponente().toLowerCase();
                if (p.getLimiteMin() != null && p.getLimiteMax() != null) {
                    if (parametroLowerCase.contains("cpu")) {
                        limiteMinCPU = p.getLimiteMin();
                        limiteMaxCPU = p.getLimiteMax();
                    }
                    if (parametroLowerCase.contains("ram")) {
                        limiteMinRAM = p.getLimiteMin();
                        limiteMaxRAM = p.getLimiteMax();
                    }
                    if (parametroLowerCase.contains("disco")) {
                        limiteMinDisco = p.getLimiteMin();
                        limiteMaxDisco = p.getLimiteMax();
                    }
                    if (parametroLowerCase.contains("processos")) {
                        limiteMinProcessos = p.getLimiteMin();
                        limiteMaxProcessos = p.getLimiteMax();
                    }
                }
                else {
                    p.setLimiteMin(0);
                    p.setLimiteMax(100);
                }
            }

            String parametrosUltrapassados = null;
            String nivelAlerta = null;
            String ts = null;
            while (entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);
                parametrosUltrapassados = "";
                nivelAlerta = "";
                if (cabecalho) {
                    saida.append("timestamp,mac,alertaJira,cpu,ram,disco,qtdProcessos,modelo,empresa\n");
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];
                    for (int i = 0; i < numeroColunasEsperadas; i++) {
                        if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty() && !valores[i].contains("dado_perdido")) {
                            valoresCompletos[i] = valores[i];
                        } else {
                            pular = true;
                        }
                    }
                    if (!pular) {
                        qtdLinhas++;
                        ts = textoLimpo(valoresCompletos[0]);
                        Double cpu = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[1])));
                        Double ram = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[2])));
                        Double disco = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[3])));
                        Integer procs = converterInteiro(textoLimpo(valoresCompletos[4]));
                        String mac = textoLimpo(valoresCompletos[5]);
                        String modelo  = textoLimpo(valoresCompletos[6]);
                        String empresa = textoLimpo(valoresCompletos[7]);

                        Boolean alertaCpu = cpu < limiteMinCPU || cpu > limiteMaxCPU;
                        Boolean alertaRam = ram < limiteMinRAM || ram > limiteMaxRAM;
                        Boolean alertaDisco = disco < limiteMinDisco || disco > limiteMaxDisco;
                        Boolean alertaProcessos = procs < limiteMinProcessos || procs > limiteMaxProcessos;
                        Boolean alerta = false;

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
                        if (alertaProcessos) {
                            qtdAlertas++;
                            parametrosUltrapassados += " Quantidade de processos, ";
                        }
                        if (qtdAlertas >= 270 && qtdLinhas % 360 == 0) {
                            alerta = true;
                            nivelAlerta = "Muito Perigoso";
                            qtdAlertas = 0;
                        } else if (qtdAlertas >= 180 && qtdLinhas % 360 == 0) {
                            alerta = true;
                            nivelAlerta = "Perigoso";
                            qtdAlertas = 0;
                        } else if (qtdAlertas >= 90 && qtdLinhas % 360 == 0) {
                            alerta = true;
                            nivelAlerta = "Atenção";
                            qtdAlertas = 0;
                        }
                        String tsFmt = formatarData(ts);
                        saida.append(tsFmt).append(",").append(mac).append(",").append(alerta).append(",")
                                .append(alertaCpu).append(",").append(alertaRam).append(",")
                                .append(alertaDisco).append(",").append(alertaProcessos).append(",")
                                .append(modelo).append(",").append(empresa).append("\n");
                    }
                    pular = false;
                }
            }
            if(!parametrosUltrapassados.isBlank()) {
                enviarMensagem("Alerta! Parâmetro(s)" + parametrosUltrapassados + "acima do limite no totem " + totem.getNumMac() + " às " + ts);
                criarChamado("Totem " + totem.getNumMac() + " acima do limite de segurança",
                        "O totem de MAC " + totem.getNumMac() +
                        " ultrapassou o(s) limite(s) estabelecidos para seus parâmetros. Nível do alerta: " + nivelAlerta + " Parâmetros ultrapassados: " + parametrosUltrapassados);
            }
            String objetoSaidaKey = "alertas/" + totem.getNumMac() + "/alertas.csv";
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_CLIENT)
                    .key(objetoSaidaKey)
                    .contentType("text/csv")
                    .build();
            s3Client.putObject(putRequest, RequestBody.fromString(saida.toString()));
            System.out.println("Arquivo de alertas enviado para S3: s3://" + BUCKET_CLIENT + "/" + objetoSaidaKey);
        } catch (NoSuchKeyException e) {
            System.out.println("Arquivo trusted não existe para o MAC " + totem.getNumMac() + ": " + e.getMessage());
            deuRuim = true;
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 Trusted!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) {
                entrada.close();
            }
            if (deuRuim) System.exit(1);
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
                SELECT t.idTotem, t.numMac, t.status, t.fkEndereco, m.idModelo, m.nome, m.descricao_arq, m.status, m.fkEmpresa
                FROM totem t
                INNER JOIN modelo m on t.fkModelo = m.idModelo;
                """, new TotemRowMapper());

        for (Totem totem : totens) {
            Modelo modelo = totem.getModelo();
            if (modelo != null) {
                List<Parametro> parametros = con.query("""
                        SELECT p.idParametro, p.limiteMin, p.limiteMax, comp.idComponente AS idTipoParametro, comp.nome AS componente, comp.status AS status
                        FROM parametro p
                        INNER JOIN componente comp ON p.fkComponente = comp.idComponente
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
                // Se o arquivo não existir, tenta criar
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
                sb.append("timestamp,cpu,ram,disco,qtdProcessos,uptime,mac,modelo,fkEmpresa\n");
            }
            for (String line : chaveParaLinha.values()) {
                sb.append(line).append("\n");
            }

            // Upload
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

    // Helper para associar o mac do totem a empresa (já que o método uploadToTrusted não recebe o objeto totem,
    // somente o mac dele)
    private static Integer buscarIdEmpresaPorMac(JdbcTemplate con, String mac) {
        try {
            return con.queryForObject(
                    "SELECT m.fkEmpresa FROM totem t JOIN modelo m ON t.fkModelo = m.idModelo WHERE t.numMac = ?",
                    Integer.class,
                    mac
            );
        } catch (Exception e) {
            System.out.println("Empresa não encontrada para MAC: " + mac);
            return null;
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
            limparDadosParaTrusted(totem.getNumMac(), con);
            limparProcessosParaTrusted(totem.getNumMac(), con);
            taxaAlertas(totem, con);
        }
        System.out.println("[1/3] Limpando RAW -> TRUSTED (Dados)...");

        System.out.println("[2/3] Limpando RAW -> TRUSTED (Processos)...");

        System.out.println("[3/3] Gerando Taxa de Alertas...");
    }
}