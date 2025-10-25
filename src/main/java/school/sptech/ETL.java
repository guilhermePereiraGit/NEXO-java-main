//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package school.sptech;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class ETL {
    static DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    static DateTimeFormatter FORMATO_SAIDA = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static Dotenv dotenv = Dotenv.load();
    private static String BUCKET_RAW;
    private static String BUCKET_TRUSTED;
    private static String BUCKET_CLIENT;
    private static Region S3_REGION;
    private static S3Client s3Client;
    private static final Map<String, Instant> ultimoTicketLocal;

    private static void limparDadosParaTrusted(String macOrigem) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();
        Connection connection = new Connection();
        JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

        try {
            GetObjectRequest getRequest = (GetObjectRequest)GetObjectRequest.builder().bucket(BUCKET_RAW).key("/registros/" + macOrigem + "/dados.csv").build();
            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Map<String, List<String>> linhasPorMac = new HashMap();
            String headerLine = null;
            String modelo = ((Modelo)associandoMacComModeloEmpresa(con, macOrigem).get(0)).getNome();
            Integer empresa = ((Modelo)associandoMacComModeloEmpresa(con, macOrigem).get(0)).getFkEmpresa();

            while(entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);
                if (cabecalho) {
                    headerLine = linha + ",modelo,idEmpresa";
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];

                    for(int i = 0; i < numeroColunasEsperadas; ++i) {
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
                    String mac = textoLimpo(valoresCompletos[5]);
                    String tsFmt = formatarData(ts);
                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + procs + "," + mac + "," + modelo + "," + empresa;
                    ((List)linhasPorMac.computeIfAbsent(mac, (k) -> new ArrayList())).add(linhaProcessada);
                }
            }

            for(Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = (String)entry.getKey();
                List<String> novasLinhas = (List)entry.getValue();
                String objetoTrustedKey = "registros/" + mac + "/dados.csv";
                mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
            }
        } catch (NoSuchKeyException e) {
            System.out.println("Arquivo RAW não existe para o MAC " + macOrigem + ": " + e.getMessage());
            deuRuim = true;
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) {
                entrada.close();
            }

            if (deuRuim) {
                System.exit(1);
            }

        }

    }

    private static void limparProcessosParaTrusted(String macOrigem) {
        Scanner entrada = null;
        Boolean deuRuim = false;
        macOrigem = macOrigem.trim();
        Connection connection = new Connection();
        JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

        try {
            GetObjectRequest getRequest = (GetObjectRequest)GetObjectRequest.builder().bucket(BUCKET_RAW).key("/registros/" + macOrigem + "/processos.csv").build();
            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            Boolean cabecalho = true;
            Integer numeroColunasEsperadas = 6;
            Map<String, List<String>> linhasPorMac = new HashMap();
            String headerLine = null;
            System.out.println(associandoMacComModeloEmpresa(con, macOrigem).get(0));
            String modelo = ((Modelo)associandoMacComModeloEmpresa(con, macOrigem).get(0)).getNome();
            Integer empresa = ((Modelo)associandoMacComModeloEmpresa(con, macOrigem).get(0)).getFkEmpresa();

            while(entrada.hasNextLine()) {
                String linha = entrada.nextLine();
                String[] valores = linha.split(",", -1);
                if (cabecalho) {
                    headerLine = linha + ",modelo,idEmpresa";
                    cabecalho = false;
                } else {
                    String[] valoresCompletos = new String[numeroColunasEsperadas];

                    for(int i = 0; i < numeroColunasEsperadas; ++i) {
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
                    String nomeProc = textoLimpo(valoresCompletos[4]).replace(",", " ");
                    String mac = textoLimpo(valoresCompletos[5]);
                    String tsFmt = formatarData(ts);
                    String linhaProcessada = tsFmt + "," + cpu + "," + ram + "," + disco + "," + nomeProc + "," + mac + "," + modelo + "," + empresa;
                    ((List)linhasPorMac.computeIfAbsent(mac, (k) -> new ArrayList())).add(linhaProcessada);
                }
            }

            for(Map.Entry<String, List<String>> entry : linhasPorMac.entrySet()) {
                String mac = (String)entry.getKey();
                List<String> novasLinhas = (List)entry.getValue();
                String objetoTrustedKey = "registros/" + mac + "/processos.csv";
                mergeAndUploadToTrustedBucket(headerLine, objetoTrustedKey, novasLinhas);
            }
        } catch (NoSuchKeyException var28) {
            System.out.println("Arquivo RAW de processos não existe para MAC " + macOrigem + ". Ignorando.");
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 RAW!");
            erro.printStackTrace();
            deuRuim = true;
        } finally {
            if (entrada != null) {
                entrada.close();
            }

            if (deuRuim) {
                System.exit(1);
            }

        }

    }

    public static void taxaAlertas(String mac) {
        Scanner entrada = null;
        boolean deuRuim = false;
        mac = mac.trim();

        try {
            String ultimaDataTicketStr = IntegracaoJira.getUltimaDataTicket(mac);
            Instant ultimaDataTicket = null;
            if (ultimaDataTicketStr != null) {
                DateTimeFormatter formatterJira = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                ultimaDataTicket = ZonedDateTime.parse(ultimaDataTicketStr, formatterJira).toInstant();
            }

            String keyTrusted = "registros/" + mac + "/dados.csv";
            GetObjectRequest getRequest = (GetObjectRequest)GetObjectRequest.builder().bucket(BUCKET_TRUSTED).key(keyTrusted).build();
            ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
            entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));
            Connection connection = new Connection();
            JdbcTemplate con = new JdbcTemplate(connection.getDataSource());
            List<Modelo> modelos = associandoMacComModeloEmpresa(con, mac);
            if (modelos != null && !modelos.isEmpty()) {
                Modelo modelo = (Modelo)modelos.get(0);
                int limiteCPU = 0;
                int limiteRAM = 0;
                int limiteDisco = 0;
                int limiteQtdProcessos = 0;
                if (modelo.getParametros() != null) {
                    for(Parametro p : modelo.getParametros()) {
                        String param = p.getTipoParametro().getNome().toLowerCase();
                        if (p.getLimite() != null) {
                            if (param.contains("cpu")) {
                                limiteCPU = p.getLimite();
                            }

                            if (param.contains("ram")) {
                                limiteRAM = p.getLimite();
                            }

                            if (param.contains("disco")) {
                                limiteDisco = p.getLimite();
                            }

                            if (param.contains("processos")) {
                                limiteQtdProcessos = p.getLimite();
                            }
                        }
                    }
                }

                boolean cabecalho = true;
                int numeroColunasEsperadas = 6;
                int qtdAlertas = 0;
                int qtdLinhas = 0;
                boolean pular = false;
                StringBuilder novasLinhas = new StringBuilder();
                String headerLine = "timestamp,mac,alertaJira,cpu,ram,disco,qtdProcessos";
                DateTimeFormatter formatterCSV = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                while(entrada.hasNextLine()) {
                    String linha = entrada.nextLine();
                    String[] valores = linha.split(",", -1);
                    if (cabecalho) {
                        cabecalho = false;
                    } else {
                        String[] valoresCompletos = new String[numeroColunasEsperadas];

                        for(int i = 0; i < numeroColunasEsperadas; ++i) {
                            if (i < valores.length && valores[i] != null && !valores[i].trim().isEmpty() && !valores[i].equals("dado_perdido")) {
                                valoresCompletos[i] = valores[i];
                            } else {
                                pular = true;
                            }
                        }

                        if (pular) {
                            pular = false;
                        } else {
                            String tsCSV = textoLimpo(valoresCompletos[0]);
                            Instant tsLinha = LocalDateTime.parse(tsCSV, formatterCSV).toInstant(ZoneOffset.UTC);
                            if (ultimaDataTicket == null || tsLinha.isAfter(ultimaDataTicket)) {
                                ++qtdLinhas;
                                double cpu = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[1])));
                                double ram = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[2])));
                                double disco = converterDouble(normalizarNumero(textoLimpo(valoresCompletos[3])));
                                int procs = converterInteiro(textoLimpo(valoresCompletos[4]));
                                String macTotem = textoLimpo(valoresCompletos[5]);
                                boolean alertaCpu = cpu >= (double)limiteCPU;
                                boolean alertaRam = ram >= (double)limiteRAM;
                                boolean alertaDisco = disco >= (double)limiteDisco;
                                boolean alertaProcessos = procs >= limiteQtdProcessos;
                                boolean alerta = false;
                                String parametrosUltrapassados = "";
                                if (alertaCpu) {
                                    ++qtdAlertas;
                                    parametrosUltrapassados = parametrosUltrapassados + " CPU,";
                                }

                                if (alertaRam) {
                                    ++qtdAlertas;
                                    parametrosUltrapassados = parametrosUltrapassados + " RAM,";
                                }

                                if (alertaDisco) {
                                    ++qtdAlertas;
                                    parametrosUltrapassados = parametrosUltrapassados + " Disco,";
                                }

                                if (alertaProcessos) {
                                    ++qtdAlertas;
                                    parametrosUltrapassados = parametrosUltrapassados + " Processos,";
                                }

                                if (qtdAlertas >= 7 && qtdLinhas % 12 == 0) {
                                    alerta = true;
                                    NotificadorSlack.enviarMensagem("Alerta! Parâmetro(s)" + parametrosUltrapassados + " acima do limite no totem " + macTotem + " às " + tsCSV);
                                    Instant ultimoLocal = (Instant)ultimoTicketLocal.get(macTotem);
                                    if (ultimoLocal == null || tsLinha.isAfter(ultimoLocal)) {
                                        try {
                                            boolean existeAberto = IntegracaoJira.existeChamadoAbertoParaMac(macTotem);
                                            if (!existeAberto) {
                                                IntegracaoJira.criarChamado("Totem " + macTotem + " acima do limite de segurança", "O totem de MAC " + macTotem + " ultrapassou o(s) limite(s): " + parametrosUltrapassados);
                                                ultimoTicketLocal.put(macTotem, tsLinha);
                                            }
                                        } catch (IOException e) {
                                            System.out.println("Erro ao consultar ou criar chamado Jira para MAC " + macTotem);
                                            e.printStackTrace();
                                        }
                                    }

                                    qtdAlertas = 0;
                                }

                                String tsFmt = formatarData(tsCSV);
                                novasLinhas.append(tsFmt).append(",").append(macTotem).append(",").append(alerta).append(",").append(alertaCpu).append(",").append(alertaRam).append(",").append(alertaDisco).append(",").append(alertaProcessos).append("\n");
                            }
                        }
                    }
                }

                String objetoClientKey = "alertas/" + mac + "/alertas.csv";
                mergeAndUploadToClientBucket(headerLine, objetoClientKey, novasLinhas.toString());
                return;
            }

            System.out.println("Nenhum modelo encontrado para o MAC " + mac);
            deuRuim = true;
        } catch (NoSuchKeyException e) {
            System.out.println("Arquivo TRUSTED não existe para o MAC " + mac + ": " + e.getMessage());
            deuRuim = true;
            return;
        } catch (Exception erro) {
            System.out.println("Erro ao acessar S3 TRUSTED!");
            erro.printStackTrace();
            deuRuim = true;
            return;
        } finally {
            if (entrada != null) {
                entrada.close();
            }

            if (deuRuim) {
                System.exit(1);
            }

        }

    }

    private static String textoLimpo(String s) {
        if (s != null && !s.trim().isEmpty()) {
            s = s.trim();
            return s;
        } else {
            return "dado_perdido";
        }
    }

    private static String normalizarNumero(String texto) {
        if (texto != null && !texto.trim().isEmpty()) {
            String numeroTratado = texto.trim();
            numeroTratado = numeroTratado.replace(",", ".");
            return numeroTratado;
        } else {
            return "dado_perdido";
        }
    }

    private static String formatarData(String texto) {
        try {
            if (texto != null && !texto.trim().isEmpty() && !texto.equalsIgnoreCase("dado_perdido")) {
                texto = texto.trim();
                LocalDateTime dt = LocalDateTime.parse(texto, FORMATO_ENTRADA);
                return dt.format(FORMATO_SAIDA);
            } else {
                return "dado_perdido";
            }
        } catch (Exception var2) {
            return "dado_perdido";
        }
    }

    private static Double converterDouble(String texto) {
        if (texto != null && !texto.trim().isEmpty()) {
            try {
                return Double.valueOf(texto.trim());
            } catch (Exception var2) {
                return (double)0.0F;
            }
        } else {
            return (double)0.0F;
        }
    }

    private static Integer converterInteiro(String texto) {
        if (texto != null && !texto.trim().isEmpty()) {
            try {
                return Integer.valueOf(texto.trim());
            } catch (Exception var2) {
                return 0;
            }
        } else {
            return 0;
        }
    }

    public static List<Modelo> associandoMacComModeloEmpresa(JdbcTemplate con, String mac) {
        List<Modelo> modeloEmpresa = con.query("SELECT m.idModelo, m.nome AS modeloNome, m.criador, m.tipo, m.descricao_arquitetura AS descricaoArquitetura,\nm.status AS statusModelo, m.fkEmpresa\nFROM totem t\nINNER JOIN modelo m ON t.fkModelo = m.idModelo\nWHERE t.numMac = ?;\n", new ModeloRowMapper(), new Object[]{mac});
        return modeloEmpresa;
    }

    public static List<Totem> carregarTotensComLimites(JdbcTemplate con) {
        List<Totem> totens = con.query("SELECT t.idTotem, t.numMac, t.instalador, t.status_totem AS statusTotem, t.dataInstalacao,\nm.idModelo, m.nome AS modeloNome, m.criador, m.tipo, m.descricao_arquitetura AS descricaoArquitetura,\nm.status AS statusModelo, m.fkEmpresa\nFROM totem t\nINNER JOIN modelo m ON t.fkModelo = m.idModelo\n", new TotemRowMapper());

        for(Totem totem : totens) {
            Modelo modelo = totem.getModelo();
            if (modelo != null) {
                List<Parametro> parametros = con.query("SELECT p.idParametro, p.limite, tp.idTipo_Parametro AS idTipoParametro, tp.nome AS nome\nFROM Parametro p\nINNER JOIN Tipo_Parametro tp ON p.fkTipoParametro = tp.idTipo_Parametro\nWHERE p.fkModelo = ?\n", new ParametroRowMapper(), new Object[]{modelo.getIdModelo()});
                modelo.setParametros(parametros);
            }
        }

        return totens;
    }

    private static void mergeAndUploadToTrustedBucket(String header, String s3Key, List<String> novasLinhas) {
        try {
            Map<String, String> chaveParaLinha = new LinkedHashMap();

            try {
                GetObjectRequest getExisting = (GetObjectRequest)GetObjectRequest.builder().bucket(BUCKET_TRUSTED).key(s3Key).build();
                ResponseInputStream<GetObjectResponse> existingStream = s3Client.getObject(getExisting);

                try (Scanner sc = new Scanner(new InputStreamReader(existingStream, StandardCharsets.UTF_8))) {
                    boolean first = true;

                    while(sc.hasNextLine()) {
                        String linhaExistente = sc.nextLine();
                        if (first) {
                            first = false;
                        } else {
                            String[] cols = linhaExistente.split(",", -1);
                            if (cols.length >= 2) {
                                String var10000 = cols[0].trim();
                                String key = var10000 + "__" + cols[cols.length - 1].trim();
                                chaveParaLinha.put(key, linhaExistente);
                            } else {
                                chaveParaLinha.put(UUID.randomUUID().toString(), linhaExistente);
                            }
                        }
                    }
                }
            } catch (NoSuchKeyException var13) {
                System.out.println("Arquivo trusted não existe. Será criado: " + s3Key);
            }

            for(String nova : novasLinhas) {
                String[] cols = nova.split(",", -1);
                if (cols.length >= 2) {
                    String var23 = cols[0].trim();
                    String key = var23 + "__" + cols[cols.length - 1].trim();
                    chaveParaLinha.put(key, nova);
                } else {
                    chaveParaLinha.put(UUID.randomUUID().toString(), nova);
                }
            }

            StringBuilder sb = new StringBuilder();
            if (header != null) {
                sb.append(header).append("\n");
            } else {
                sb.append("timestamp,cpu,ram,disco,qtdProcessos,mac\n");
            }

            for(String line : chaveParaLinha.values()) {
                sb.append(line).append("\n");
            }

            PutObjectRequest putReq = (PutObjectRequest)PutObjectRequest.builder().bucket(BUCKET_TRUSTED).key(s3Key).contentType("text/csv").build();
            s3Client.putObject(putReq, RequestBody.fromString(sb.toString(), StandardCharsets.UTF_8));
            System.out.println("Upload/merge OK: s3://" + BUCKET_TRUSTED + "/" + s3Key);
        } catch (Exception e) {
            System.out.println("Erro durante merge/upload para trusted: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void mergeAndUploadToClientBucket(String header, String key, String novasLinhas) {
        try {
            StringBuilder conteudoFinal = new StringBuilder();
            boolean arquivoExiste = true;

            try {
                GetObjectRequest getRequest = (GetObjectRequest)GetObjectRequest.builder().bucket(BUCKET_CLIENT).key(key).build();
                ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
                Scanner entradaExistente = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

                while(entradaExistente.hasNextLine()) {
                    conteudoFinal.append(entradaExistente.nextLine()).append("\n");
                }

                entradaExistente.close();
            } catch (NoSuchKeyException var8) {
                arquivoExiste = false;
            }

            if (!arquivoExiste) {
                conteudoFinal.append(header).append("\n");
            }

            conteudoFinal.append(novasLinhas);
            s3Client.putObject((PutObjectRequest)PutObjectRequest.builder().bucket(BUCKET_CLIENT).key(key).build(), RequestBody.fromString(conteudoFinal.toString()));
            System.out.println("Upload/append realizado com sucesso no bucket CLIENT: " + key);
        } catch (Exception e) {
            System.out.println("Erro ao fazer merge/upload para bucket CLIENT!");
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Connection connection = new Connection();
        JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

        for(Totem totem : carregarTotensComLimites(con)) {
            limparDadosParaTrusted(totem.getNumMac());
            limparProcessosParaTrusted(totem.getNumMac());
            taxaAlertas(totem.getNumMac());
        }

        System.out.println("[1/3] Limpando RAW -> TRUSTED (Dados)...");
        System.out.println("[2/3] Limpando RAW -> TRUSTED (Processos)...");
        System.out.println("[3/3] TRUSTED -> CLIENT (Taxa de Alertas)...");
    }

    static {
        BUCKET_RAW = dotenv.get("BUCKET_RAW");
        BUCKET_TRUSTED = dotenv.get("BUCKET_TRUSTED");
        BUCKET_CLIENT = dotenv.get("BUCKET_CLIENT");
        S3_REGION = Region.US_EAST_1;

        try {
            s3Client = (S3Client)((S3ClientBuilder)((S3ClientBuilder)S3Client.builder().region(S3_REGION)).credentialsProvider(DefaultCredentialsProvider.create())).build();
            System.out.println("Cliente S3 inicializado com DefaultCredentialsProvider (EC2 Role).");
        } catch (Exception e) {
            System.out.println("Erro ao criar cliente S3!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        ultimoTicketLocal = new HashMap();
    }
}
