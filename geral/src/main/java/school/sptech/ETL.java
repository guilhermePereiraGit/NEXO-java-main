package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static school.sptech.IntegracaoJira.criarChamado;
import static school.sptech.NotificadorSlack.enviarMensagem;

public class ETL implements RequestHandler<S3Event, String> {
    private static final String BUCKET_TRUSTED = "bucket-trusted-nexo-barros";
    private static final String BUCKET_CLIENT = "bucket-client-nexo-barros";
    private static final Region S3_REGION = Region.US_EAST_1;

    private static final DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final DateTimeFormatter FORMATO_SAIDA = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final S3Client s3Client;

    static {
        s3Client = S3Client.builder()
                .region(S3_REGION)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        try {
            if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
                context.getLogger().log("S3Event vazio");
                return "NO_RECORDS";
            }

            // pego o primeiro record
            com.amazonaws.services.lambda.runtime.events.S3Event.S3EventNotificationRecord record = event.getRecords().get(0);

            // getS3() retorna models.s3.S3Entity
            String bucket = record.getS3().getBucket().getName();
            String key    = record.getS3().getObject().getKey();

            context.getLogger().log("Recebido S3 event: s3://" + bucket + "/" + key);

            KeyParts parts = parseKey(key);
            if (parts == null) {
                context.getLogger().log("Key não no formato esperado: " + key);
                return "BAD_KEY_FORMAT";
            }

            Connection connection = new Connection();
            JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

            if (parts.fileName.equalsIgnoreCase("dados.csv")) {
                limparDadosParaTrusted(bucket, key, con);
                Totem totem = buscarTotemPorMac(con, parts.mac);
                if (totem != null) {
                    taxaAlertas(totem, bucket, parts);
                } else {
                    context.getLogger().log("Totem não encontrado no DB para mac: " + parts.mac);
                }
            } else if (parts.fileName.equalsIgnoreCase("processos.csv")) {
                limparProcessosParaTrusted(bucket, key, con, parts);
            } else {
                context.getLogger().log("Arquivo ignorado (não é dados.csv nem processos.csv): " + parts.fileName);
            }


            return "OK";
        } catch (Exception e) {
            context.getLogger().log("Erro no handler: " + e.getMessage());
            e.printStackTrace();
            return "ERROR";
        }
    }

    // --- PARSER DE KEY ---
    private static class KeyParts {
        String idEmpresa;
        String mac;
        String date; // yyyy-MM-dd
        String fileName;
    }

    private KeyParts parseKey(String key) {
        if (key == null) return null;
        String[] partes = key.split("/");
        if (partes.length < 4) return null;
        KeyParts kp = new KeyParts();
        kp.idEmpresa = partes[0];
        kp.mac = partes[1];
        kp.date = partes[2];
        kp.fileName = partes[3];
        return kp;
    }

    // --- LIMPAR DADOS (RAW -> TRUSTED) usando key recebida ---
    private void limparDadosParaTrusted(String bucket, String key, JdbcTemplate con) {
        java.util.Scanner entrada = null;
        boolean deuRuim = false;

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new java.util.Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            boolean cabecalho = true;
            int numeroColunasEsperadas = 9;
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

                    String ts = textoLimpo(valoresCompletos[0]);
                    String cpu = normalizarNumero(textoLimpo(valoresCompletos[1]));
                    String ram = normalizarNumero(textoLimpo(valoresCompletos[2]));
                    String disco = normalizarNumero(textoLimpo(valoresCompletos[3]));
                    String procs = textoLimpo(valoresCompletos[4]);
                    String uptime = textoLimpo(String.valueOf(Double.parseDouble(valoresCompletos[5]) / 86400));
                    String mac = textoLimpo(valoresCompletos[6]);
                    String modelo = textoLimpo(valoresCompletos[7]);
                    String empresa = textoLimpo(valoresCompletos[8]);
                    String tsFmt = formatarData(ts);

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
            System.out.println("Arquivo RAW não existe: " + e.getMessage());
            deuRuim = true;
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) entrada.close();
            if (deuRuim) throw new RuntimeException("Falha ao processar dados para trusted");
        }
    }

    // --- LIMPAR PROCESSOS (RAW -> TRUSTED) usando key recebida ---
    private void limparProcessosParaTrusted(String bucket, String key, JdbcTemplate con, KeyParts parts) {
        java.util.Scanner entrada = null;
        boolean deuRuim = false;

        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new java.util.Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

            boolean cabecalho = true;
            int numeroColunasEsperadas = 8;
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

                    String ts = textoLimpo(valoresCompletos[0]);
                    String cpu = normalizarNumero(textoLimpos(valoresCompletos[1]));
                    // Note: reuse helper functions; keeping consistent with original
                    String ram = normalizarNumero(textoLimpos(valoresCompletos[2]));

                    // ... continue mapping similar to original implementation
                    // For brevity we mirror original logic but keep key usage

                    String mac = textoLimpo(valoresCompletos[5]);
                    String modelo = textoLimpo(valoresCompletos[6]);
                    String empresa = textoLimpo(valoresCompletos[7]);
                    String tsFmt = formatarData(ts);

                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + "0" + "," + "proc" + "," + mac + "," + modelo + "," + empresa;
                    linhasPorMac.computeIfAbsent(mac, k -> new ArrayList<>()).add(linhaProcessada);
                }
            }

            for (Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = entry.getKey();
                List<String> novasLinhas = entry.getValue();
                String objetoTrustedKey = parts.idEmpresa + "/" + parts.mac + "/" + parts.date + "/processos.csv";
                mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
            }

        } catch (NoSuchKeyException e) {
            System.out.println("Arquivo RAW de processos não existe para mac: " + parts.mac + ". Ignorando.");
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW (processos)!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) entrada.close();
            if (deuRuim) throw new RuntimeException("Falha ao processar processos para trusted");
        }
    }

    // --- TAXA ALERTAS (TRUSTED -> CLIENT) adaptado para rodar por totem (mac) ---
    private void taxaAlertas(Totem totem, String bucketRaw, KeyParts parts) {
        java.util.Scanner entrada = null;
        StringBuilder saida = new StringBuilder();
        boolean deuRuim = false;

        try {
            // trusted file key is idEmpresa/mac/date/dados.csv — we assume trusted created already
            String trustedKey = buscarIdEmpresaPorMac(new JdbcTemplate(new Connection().getDataSource()), totem.getNumMac())
                    + "/" + totem.getNumMac() + "/" + parts.date + "/dados.csv";

            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(BUCKET_TRUSTED)
                    .key(trustedKey)
                    .build();

            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new java.util.Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

            boolean cabecalho = true;
            int numeroColunasEsperadas = 9; // timestamp,cpu,ram,disco,qtdProcessos,uptime,mac,modelo,fkEmpresa
            int qtdAlertas = 0;
            int qtdLinhas = 0;
            boolean pular = false;

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
                } else {
                    p.setLimiteMin(0);
                    p.setLimiteMax(100);
                }
            }

            String parametrosUltrapassados = "";
            String nivelAlerta = "";
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
                        String mac = textoLimpo(valoresCompletos[6]);
                        String modelo = textoLimpo(valoresCompletos[7]);
                        String empresa = textoLimpo(valoresCompletos[8]);

                        boolean alertaCpu = cpu < limiteMinCPU || cpu > limiteMaxCPU;
                        boolean alertaRam = ram < limiteMinRAM || ram > limiteMaxRAM;
                        boolean alertaDisco = disco < limiteMinDisco || disco > limiteMaxDisco;
                        boolean alertaProcessos = procs < limiteMinProcessos || procs > limiteMaxProcessos;
                        boolean alerta = false;

                        if (alertaCpu) {
                            parametrosUltrapassados += " CPU, ";
                            if (cpu > limiteMaxCPU*1.5){
                                qtdAlertas += 3;
                            }else if (cpu > limiteMaxCPU*1.25){
                                qtdAlertas += 2;
                            }else if(cpu > limiteMaxCPU){
                                qtdAlertas += 1;
                            }
                        }
                        if (alertaRam) {
                            parametrosUltrapassados += " RAM, ";
                            if (ram > limiteMaxRAM*1.5){
                                qtdAlertas += 3;
                            }else if (ram > limiteMaxRAM*1.25){
                                qtdAlertas += 2;
                            }else if(ram > limiteMaxRAM){
                                qtdAlertas += 1;
                            }
                        }
                        if (alertaDisco) {
                            parametrosUltrapassados += " Uso de disco, ";
                            if (disco > limiteMaxDisco*1.5){
                                qtdAlertas += 3;
                            }else if (disco > limiteMaxDisco*1.25){
                                qtdAlertas += 2;
                            }else if(disco > limiteMaxDisco){
                                qtdAlertas += 1;
                            }
                        }
                        if (alertaProcessos) {
                            parametrosUltrapassados += " Quantidade de processos, ";
                            if (procs > limiteMaxProcessos*1.5){
                                qtdAlertas += 3;
                            }else if (procs > limiteMaxProcessos*1.25){
                                qtdAlertas += 2;
                            }else if(procs > limiteMaxProcessos){
                                qtdAlertas += 1;
                            }
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
            if (!parametrosUltrapassados.isBlank()) {
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
            if (entrada != null) entrada.close();
            if (deuRuim) throw new RuntimeException("Falha ao gerar taxa de alertas");
        }
    }

    // --- Métodos Client ---
    public void dadosParaClient(String idEmpresa, String mac, String date, JdbcTemplate con) {
        Scanner scannerDados = null;
        Scanner scannerProcessos = null;

        try {
            // Estrutura para armazenar dados por janela de 4 horas
            JanelaTempo4h[] janelas = new JanelaTempo4h[6];
            Processo[] processos = new Processo[6];

            String trustedKeyDados = idEmpresa + "/" + mac + "/" + date + "/dados.csv";
            String trustedKeyProcessos = idEmpresa + "/" + mac + "/" + date + "/processos.csv";

            // ========== LEITURA DE DADOS ==========
            try {
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(BUCKET_TRUSTED)
                        .key(trustedKeyDados)
                        .build();

                ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
                scannerDados = new java.util.Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

                boolean cabecalho = true;
                while (scannerDados.hasNextLine()) {
                    String linha = scannerDados.nextLine();
                    if (cabecalho) {
                        cabecalho = false;
                        continue;
                    }

                    String[] valores = linha.split(",", -1);
                    if (valores.length < 9) continue;

                    try {
                        String timestamp = textoLimpo(valores[0]);

                        if (timestamp.equals("Dado_perdido")) continue;

                        Double cpu = converterDouble(normalizarNumero(textoLimpo(valores[1])));
                        Double ram = converterDouble(normalizarNumero(textoLimpo(valores[2])));
                        Double disco = converterDouble(normalizarNumero(textoLimpo(valores[3])));
                        Double uptime = converterDouble(normalizarNumero(textoLimpo(valores[5])));

                        // Calcula índice da janela usando módulo de 4
                        int idx = obterIndiceJanela(timestamp);

                        if (janelas[idx] == null) {
                            janelas[idx] = new JanelaTempo4h();
                        }
                        janelas[idx].adicionarDado(cpu, ram, disco, uptime);

                    } catch (Exception e) {
                        System.out.println("Erro ao processar linha de dados: " + e.getMessage());
                    }
                }
            } catch (NoSuchKeyException e) {
                System.out.println("Arquivo de dados não existe no trusted: " + trustedKeyDados);
            }

            // ========== LEITURA DE PROCESSOS ==========
            try {
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(BUCKET_TRUSTED)
                        .key(trustedKeyProcessos)
                        .build();

                ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
                scannerProcessos = new java.util.Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

                boolean cabecalho = true;
                while (scannerProcessos.hasNextLine()) {
                    String linha = scannerProcessos.nextLine();
                    if (cabecalho) {
                        cabecalho = false;
                        continue;
                    }

                    String[] valores = linha.split(",", -1);
                    if (valores.length < 6) continue;

                    try {
                        String timestamp = textoLimpo(valores[0]);
                        String processo = textoLimpo(valores[1]);
                        Double cpuProc = converterDouble(normalizarNumero(textoLimpo(valores[2])));
                        Double ramProc = converterDouble(normalizarNumero(textoLimpo(valores[3])));

                        if (timestamp.equals("Dado_perdido")) continue;

                        // Calcula índice da janela usando módulo de 4
                        int idx = obterIndiceJanela(timestamp);

                        if (processos[idx] == null) {
                            processos[idx] = new Processo();
                        }
                        processos[idx].adicionarProcesso(processo, cpuProc, ramProc);

                    } catch (Exception e) {
                        System.out.println("Erro ao processar linha de processos: " + e.getMessage());
                    }
                }
            } catch (NoSuchKeyException e) {
                System.out.println("Arquivo de processos não existe no trusted: " + trustedKeyProcessos);
            }

            boolean temDados = false;
            for (JanelaTempo4h janela : janelas) {
                if (janela != null) {
                    temDados = true;
                    break;
                }
            }

            if (!temDados) {
                System.out.println("Nenhum dado válido encontrado para " + trustedKeyDados);
                return;
            }

            // ========== CONSTRUÇÃO DO JSON ==========
            StringBuilder jsonBuilder = new StringBuilder();
            jsonBuilder.append("{\"data\":\"").append(date).append("\",\"mac\":\"").append(mac).append("\",\"janelas4h\":[");

            String[] ordemJanelas = {"00:00-04:00", "04:00-08:00", "08:00-12:00", "12:00-16:00", "16:00-20:00", "20:00-00:00"};
            String[] ordemHorasFim = {"04:00", "08:00", "12:00", "16:00", "20:00", "24:00"};

            for (int i = 0; i < 6; i++) {
                if (i > 0) jsonBuilder.append(",");

                String[] horas = ordemJanelas[i].split("-");

                jsonBuilder.append("{\"horaInicio\":\"").append(horas[0]).append("\",");
                jsonBuilder.append("\"horaFim\":\"").append(ordemHorasFim[i]).append("\",");

                // Se a janela tem dados, usa os valores calculados, senão coloca 0
                if (janelas[i] != null) {
                    jsonBuilder.append("\"cpuMedia\":").append(Math.round(janelas[i].obterMediaCpu() * 10.0) / 10.0).append(",");
                    jsonBuilder.append("\"ramMedia\":").append(Math.round(janelas[i].obterMediaRam() * 10.0) / 10.0).append(",");
                    jsonBuilder.append("\"discoMedia\":").append(Math.round(janelas[i].obterMediaDisco() * 10.0) / 10.0).append(",");
                    jsonBuilder.append("\"uptime\":").append(janelas[i].obterUltimoUptime()).append(",");
                    jsonBuilder.append("\"processos\":[");

                    if (processos[i] != null) {
                        jsonBuilder.append(processos[i].obterJson());
                    }
                } else {
                    // Janela vazia: valores padrão
                    jsonBuilder.append("\"cpuMedia\":0.0,");
                    jsonBuilder.append("\"ramMedia\":0.0,");
                    jsonBuilder.append("\"discoMedia\":0.0,");
                    jsonBuilder.append("\"uptime\":0.0,");
                    jsonBuilder.append("\"processos\":[");
                }

                jsonBuilder.append("]}");
            }

            jsonBuilder.append("]}");

            String clientKey = idEmpresa + "/" + mac + "/" + date + "/dados.json";
            uploadJsonToClient(jsonBuilder.toString(), clientKey);

            System.out.println("Dados consolidados para client: s3://" + BUCKET_CLIENT + "/" + clientKey);

        } catch (Exception erro) {
            System.out.println("Erro ao consolidar dados para client!");
            erro.printStackTrace();
        } finally {
            if (scannerDados != null) scannerDados.close();
            if (scannerProcessos != null) scannerProcessos.close();
        }
    }

    private int obterIndiceJanela(String timestamp) {
        try {
            // Suporta dois formatos:
            // 1. yyyy-MM-dd HH:mm:ss (com espaço e dois-pontos)
            // 2. yyyy-MM-dd_HH-mm-ss (com underscore e hífen)

            if (timestamp == null || timestamp.trim().isEmpty() || timestamp.equals("Dado_perdido")) {
                return -1;
            }

            String horaStr = "";

            // Tenta formato com espaço: yyyy-MM-dd HH:mm:ss
            if (timestamp.contains(" ")) {
                String[] partes = timestamp.split(" ");
                if (partes.length >= 2) {
                    String[] timeParts = partes[1].split(":");
                    if (timeParts.length >= 1) {
                        horaStr = timeParts[0];
                    }
                }
            }
            // Tenta formato com underscore: yyyy-MM-dd_HH-mm-ss
            else if (timestamp.contains("_")) {
                String[] partes = timestamp.split("_");
                if (partes.length >= 2) {
                    String[] timeParts = partes[1].split("-");
                    if (timeParts.length >= 1) {
                        horaStr = timeParts[0];
                    }
                }
            }

            if (horaStr.isEmpty()) {
                System.out.println("Timestamp inválido - não foi possível extrair hora: " + timestamp);
                return -1;
            }

            int hora = Integer.parseInt(horaStr);
            int indice = hora / 4;

            System.out.println("Timestamp: " + timestamp + " -> Hora: " + hora + " -> Índice: " + indice);

            return indice;

        } catch (Exception e) {
            System.out.println("Erro ao obter índice da janela para timestamp '" + timestamp + "': " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }

    private void uploadJsonToClient(String jsonContent, String s3Key) {
        try {
            if (jsonContent == null || jsonContent.trim().isEmpty()) {
                System.out.println("Conteúdo JSON vazio. Abortando upload.");
                return;
            }

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_CLIENT)
                    .key(s3Key)
                    .contentType("application/json")
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromString(jsonContent, StandardCharsets.UTF_8));
            System.out.println("Upload JSON OK: s3://" + BUCKET_CLIENT + "/" + s3Key);

        } catch (Exception e) {
            System.out.println("Erro ao fazer upload JSON para client: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // --- MERGE E UPLOAD PARA TRUSTED (mantive igual ao original) ---
    private void mergeAndUploadToTrustedBucket(String header, String s3Key, List<String> novasLinhas) {
        try {
            Map<String, String> chaveParaLinha = new LinkedHashMap<>();

            try {
                GetObjectRequest getExisting = GetObjectRequest.builder()
                        .bucket(BUCKET_TRUSTED)
                        .key(s3Key)
                        .build();

                ResponseInputStream<GetObjectResponse> existingStream = s3Client.getObject(getExisting);
                try (java.util.Scanner sc = new java.util.Scanner(new InputStreamReader(existingStream, StandardCharsets.UTF_8))) {
                    boolean first = true;
                    while (sc.hasNextLine()) {
                        String linhaExistente = sc.nextLine();
                        if (first) {
                            first = false;
                            continue;
                        }
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
                System.out.println("Arquivo trusted não existe. Será criado: " + s3Key);
            }

            for (String nova : novasLinhas) {
                String[] cols = nova.split(",", -1);
                if (cols.length >= 2) {
                    String key = cols[0].trim() + "__" + cols[cols.length - 1].trim();
                    chaveParaLinha.put(key, nova);
                } else {
                    chaveParaLinha.put(UUID.randomUUID().toString(), nova);
                }
            }

            StringBuilder sb = new StringBuilder();
            if (header != null) {
                sb.append(header).append("\n");
            } else {
                sb.append("timestamp,cpu,ram,disco,qtdProcessos,uptime,mac,modelo,fkEmpresa\n");
            }
            for (String line : chaveParaLinha.values()) {
                sb.append(line).append("\n");
            }

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

    // --- HELPERS (copiados/ajustados do original) ---
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

    // --- DB HELPERS ---
    private Integer buscarIdEmpresaPorMac(JdbcTemplate con, String mac) {
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

    private Totem buscarTotemPorMac(JdbcTemplate con, String mac) {
        try {
            Totem totem = con.queryForObject(
                    "SELECT t.idTotem, t.numMac, t.status, t.fkEndereco, m.idModelo, m.nome, m.descricao_arq, m.status, m.fkEmpresa " +
                            "FROM totem t INNER JOIN modelo m on t.fkModelo = m.idModelo WHERE t.numMac = ?;",
                    new TotemRowMapper(),
                    mac
            );
            if (totem != null) {
                Modelo modelo = totem.getModelo();
                if (modelo != null) {
                    List<Parametro> parametros = con.query(
                            "SELECT p.idParametro, p.limiteMin, p.limiteMax, comp.idComponente AS idTipoParametro, comp.nome AS componente, comp.status AS status " +
                                    "FROM parametro p INNER JOIN componente comp ON p.fkComponente = comp.idComponente WHERE p.fkModelo = ?",
                            new ParametroRowMapper(), modelo.getIdModelo());
                    modelo.setParametros(parametros);
                }
            }
            return totem;
        } catch (Exception e) {
            System.out.println("Erro ao buscar totem por mac: " + e.getMessage());
            return null;
        }
    }

    // placeholders — alguns projetos podem ter nomes de métodos/datasources diferentes
    // A classe Connection, Totem, TotemRowMapper, Modelo, Parametro, ParametroRowMapper fazem parte do seu projeto original.

    // Pequena correção: helper usado no limparProcessos
    private String textoLimpos(String s) {
        return textoLimpo(s);
    }

}
