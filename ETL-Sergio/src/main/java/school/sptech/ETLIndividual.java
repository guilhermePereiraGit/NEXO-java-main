package school.sptech;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETLIndividual {
    private static final String AWS_ACCESS_KEY = "ASIAS4XUDPRVWEAGLD3D";
    private static final String AWS_SECRET_KEY = "3mSYKGSdjCb1sMu94tWaq48P46/cFKtsapKDXmh+";
    private static final String AWS_SESSION_TOKEN = "IQoJb3JpZ2luX2VjEE4aCXVzLXdlc3QtMiJIMEYCIQDUXBnq85pCveTgLO2J22OPtUpSc5d9Af5Stiwx3gQvAAIhAN/3LkEoG/Jln+FncxWOVnDJTIH6mipYIAtPHcJ42kmVKrYCCBcQABoMMTk5MTU0MTcwOTg3IgxbUEY8yaZx3DhCUdsqkwIa+Ieh+4sje70RDwTenZNIrxniMDYV5yBvRiRWgB2NC807XO3y78WeNKaPiPn3bAEkZjDh8T9E57I1KwWgt3Q3EYl7TH9ttJcpOcz5QEEeqzhklEOBmUt55v8I8BQCs9EdL713VgdWMPg4c8WpnWlAk6b7CBO3g+fzc0BYpX1SJke8eCh6lfz2J1xXBZdcSVYZHibld3nj3diQXOjz23raJ8CQuf/+LVwIcf7KVVUO3ApD3b4qjo/JpPNgM6J1b5+HRgwAOir0Sc+Z/gP9inqvMmoxqwfX1d6WZM0MFlmq4CgK6pod7ntxrqQ8XIXzXf9vhC6BcWd5jvTCL2SGb+hQVG7Fzfa+SoiFhBs5uOn5E7BkbDDd47vJBjqcAbbseEQdWtI7yo1wRZrWiVH8K7IBdxeDaEGVRz5fHaKEPrVugr75lstZ8awt0dZUXnHEAkXDwr5YdLB5eEX7Crv5sAJI+LyuHdWQ07tt6q+2WUmk10jgT1hKpD055fvPH5IQdbgiHQi+qrY0UWxsRQQVZfjtgf2a2b5Kq2hxCYShbxZrjqIM3ofjDyo88cS8vdQT2QzJpXKczzYugg==";
    private static final String BUCKET_TRUSTED = "bucket-trusted-nexo";
    private static final String BUCKET_CLIENT = "bucket-client-nexo";

    private static final Region S3_REGION = Region.US_EAST_1;

    private static final DateTimeFormatter FORMATO_DATA_PASTA =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static void main(String[] args) {

        System.out.println("ETL INDIVIDUAL - NEXO (Sergio)");

        Connection conexao = null;

        try {
            conexao = school.sptech.Connection.getConnection();

            if (conexao == null) {
                System.out.println("ETL: Conexão com o banco retornou nula. Encerrando.");
                return;
            }
            AwsCredentialsProvider credenciais = StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(
                            AWS_ACCESS_KEY,
                            AWS_SECRET_KEY,
                            AWS_SESSION_TOKEN
                    )
            );
            S3Client s3 = S3Client.builder()
                    .region(S3_REGION)
                    .credentialsProvider(credenciais)
                    .build();


            // 1) Busca todas as empresas no banco
            List<Integer> listaEmpresas = buscarTodasEmpresas(conexao);
            // 2) Para cada empresa
            for (Integer idEmpresaAtual : listaEmpresas) {

                System.out.println("-------------------------------------------------");
                System.out.println("Processando empresa ID: " + idEmpresaAtual);


                // 3) Busca todos os MACs dessa empresa no bucket TRUSTED
                List<String> listaMacsEmpresa = buscarTotensDoBucket(s3, idEmpresaAtual);

                // 4) Para cada MAC dessa empresa
                for (String macDoTotem : listaMacsEmpresa) {
                    System.out.println("-> Empresa " + idEmpresaAtual + " | MAC " + macDoTotem);

                    DashboardData dados = new DashboardData();

                    // Busca modelo no BANCO usando o MAC
                    Integer idModelo = buscarInfoModelo(conexao, dados, idEmpresaAtual, macDoTotem);

                    // Se nao encontrar modelo no banco, pula o totem
                    if (idModelo == null) {
                        System.out.println(
                                "BD: Modelo não encontrado para o MAC "  + macDoTotem  + " — pulando este totem."
                        );
                        continue; // vai para o próximo MAC
                    }

                    // Só chega aqui se encontrou modelo no banco
                    buscarParametrosModelo(conexao, idModelo, dados);
                    executarLeituraS3(dados, s3, idEmpresaAtual, macDoTotem);
                    salvarJsonNoS3(dados, idEmpresaAtual, macDoTotem);
                }
            }

        } catch (Exception e) {
            System.out.println("ETL: Erro geral na ETL: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (conexao != null) {
                    conexao.close();
                }
            } catch (Exception e) {
                System.out.println("ETL: Erro ao fechar conexão com o banco.");
            }
        }

        System.out.println("ETL 30 Dias FINALIZADA!");
    }

    public static List<Integer> buscarTodasEmpresas(Connection conexao) {

        List<Integer> listaEmpresas = new ArrayList<>();

        PreparedStatement comandoBuscarEmpresas = null;
        ResultSet resultadoEmpresas = null;

        try {
            String sqlBuscarEmpresas =
                    "SELECT idEmpresa FROM empresa";

            comandoBuscarEmpresas = conexao.prepareStatement(sqlBuscarEmpresas);
            resultadoEmpresas = comandoBuscarEmpresas.executeQuery();

            while (resultadoEmpresas.next()) {
                Integer idEmpresa = resultadoEmpresas.getInt("idEmpresa");
                listaEmpresas.add(idEmpresa);
            }

        } catch (Exception e) {
            System.out.println("BD: Erro ao buscar empresas: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (resultadoEmpresas != null) resultadoEmpresas.close();
                if (comandoBuscarEmpresas != null) comandoBuscarEmpresas.close();
            } catch (Exception e) {
                System.out.println("BD: Erro ao fechar recursos de empresas.");
            }
        }

        return listaEmpresas;
    }

    public static List<String> buscarTotensDoBucket(S3Client s3, Integer idEmpresaAtual) {

        List<String> listaMacs = new ArrayList<>();

        try {
            String prefixoEmpresa = idEmpresaAtual + "/";

            ListObjectsV2Request requisicaoListagem = ListObjectsV2Request.builder()
                    .bucket(BUCKET_TRUSTED)
                    .prefix(prefixoEmpresa)
                    .build();

            ListObjectsV2Response respostaListagem = s3.listObjectsV2(requisicaoListagem);
            List<S3Object> arquivosEncontrados = respostaListagem.contents();

            for (S3Object arquivo : arquivosEncontrados) {
                String caminhoNoBucket = arquivo.key();

                String[] partes = caminhoNoBucket.split("/");

                // partes 0 = idEmpresa, 1 = MAC, 2 = data, 3 = dados.csv
                if (partes.length >= 2) {
                    String macDoTotem = partes[1];

                    if (!listaMacs.contains(macDoTotem)) {
                        listaMacs.add(macDoTotem);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("S3: Erro ao buscar MACs no bucket: " + e.getMessage());
            e.printStackTrace();
        }

        return listaMacs;
    }

    public static Integer buscarInfoModelo(Connection conexao,
                                           DashboardData dados,
                                           Integer idEmpresaAtual,
                                           String macDoTotem) {

        Integer idModeloEncontrado = null;
        Integer totalTotensDoModelo = 0;
        Integer totalTotensDaEmpresa = 0;
        String nomeModelo = "Desconhecido";

        PreparedStatement comandoBuscarModelo = null;
        ResultSet resultadoModelo = null;

        PreparedStatement comandoContarTotensModelo = null;
        ResultSet resultadoContagemTotensModelo = null;

        PreparedStatement comandoContarTotensEmpresa = null;
        ResultSet resultadoContagemTotensEmpresa = null;

        try {
            String sqlBuscarModelo =
                    "SELECT m.idModelo, m.nome " +
                            "FROM totem t " +
                            "JOIN modelo m ON t.fkModelo = m.idModelo " +
                            "WHERE t.numMAC = ?";

            comandoBuscarModelo = conexao.prepareStatement(sqlBuscarModelo);
            comandoBuscarModelo.setString(1, macDoTotem);

            resultadoModelo = comandoBuscarModelo.executeQuery();

            if (resultadoModelo.next()) {
                idModeloEncontrado = resultadoModelo.getInt("idModelo");
                nomeModelo = resultadoModelo.getString("nome");
            }

            if (idModeloEncontrado != null) {
                String sqlContarTotensModelo =
                        "SELECT COUNT(*) AS total " +
                                "FROM totem " +
                                "WHERE fkModelo = ?";

                comandoContarTotensModelo = conexao.prepareStatement(sqlContarTotensModelo);
                comandoContarTotensModelo.setInt(1, idModeloEncontrado);

                resultadoContagemTotensModelo = comandoContarTotensModelo.executeQuery();

                if (resultadoContagemTotensModelo.next()) {
                    totalTotensDoModelo = resultadoContagemTotensModelo.getInt("total");
                }
            }

            String sqlContarTotensEmpresa =
                    "SELECT COUNT(*) AS total " +
                            "FROM totem t " +
                            "JOIN modelo m ON t.fkModelo = m.idModelo " +
                            "WHERE m.fkEmpresa = ?";

            comandoContarTotensEmpresa = conexao.prepareStatement(sqlContarTotensEmpresa);
            comandoContarTotensEmpresa.setInt(1, idEmpresaAtual);

            resultadoContagemTotensEmpresa = comandoContarTotensEmpresa.executeQuery();

            if (resultadoContagemTotensEmpresa.next()) {
                totalTotensDaEmpresa = resultadoContagemTotensEmpresa.getInt("total");
            }

            dados.setNomeModelo(nomeModelo);
            dados.setTotalMonitorado(totalTotensDoModelo);
            dados.setTotalTotensEmpresa(totalTotensDaEmpresa);

            Double percentualModeloNoParque = 0.0;
            if (totalTotensDaEmpresa != null && totalTotensDaEmpresa > 0) {
                percentualModeloNoParque =
                        (totalTotensDoModelo * 100.0) / totalTotensDaEmpresa;
            }
            dados.setPercentualModeloParque(percentualModeloNoParque);

        } catch (Exception e) {
            System.out.println("BD: Erro ao buscar modelo / totens: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (resultadoModelo != null) resultadoModelo.close();
                if (comandoBuscarModelo != null) comandoBuscarModelo.close();

                if (resultadoContagemTotensModelo != null) resultadoContagemTotensModelo.close();
                if (comandoContarTotensModelo != null) comandoContarTotensModelo.close();

                if (resultadoContagemTotensEmpresa != null) resultadoContagemTotensEmpresa.close();
                if (comandoContarTotensEmpresa != null) comandoContarTotensEmpresa.close();
            } catch (Exception e) {
                System.out.println("BD: Erro ao fechar recursos de modelo.");
            }
        }

        return idModeloEncontrado;
    }

    public static void buscarParametrosModelo(Connection conexao,
                                              Integer idModelo,
                                              DashboardData dados) {

        if (idModelo == null) {
            System.out.println("BD: idModelo está nulo, não dá para buscar parâmetros.");
            return;
        }

        PreparedStatement comandoBuscarParametros = null;
        ResultSet resultadoParametros = null;

        try {
            String sqlBuscarParametros =
                    "SELECT c.nome AS nomeComponente, p.limiteMin, p.limiteMax " +
                            "FROM parametro p " +
                            "JOIN componente c ON p.fkComponente = c.idComponente " +
                            "WHERE p.fkModelo = ?";

            comandoBuscarParametros = conexao.prepareStatement(sqlBuscarParametros);
            comandoBuscarParametros.setInt(1, idModelo);

            resultadoParametros = comandoBuscarParametros.executeQuery();

            Double limiteMinimoCpu = null;
            Double limiteMaximoCpu = null;

            Double limiteMinimoRam = null;
            Double limiteMaximoRam = null;

            Double limiteMinimoDisco = null;
            Double limiteMaximoDisco = null;

            Double limiteMinimoProcessos = null;
            Double limiteMaximoProcessos = null;

            while (resultadoParametros.next()) {
                String nomeComponente = resultadoParametros.getString("nomeComponente");
                Double limiteMinimo = resultadoParametros.getDouble("limiteMin");
                Double limiteMaximo = resultadoParametros.getDouble("limiteMax");

                if (nomeComponente == null) {
                    continue;
                }

                String nomeComponenteMaiusculo = nomeComponente.toUpperCase();

                if (nomeComponenteMaiusculo.contains("CPU")) {
                    limiteMinimoCpu = limiteMinimo;
                    limiteMaximoCpu = limiteMaximo;
                } else if (nomeComponenteMaiusculo.contains("RAM")) {
                    limiteMinimoRam = limiteMinimo;
                    limiteMaximoRam = limiteMaximo;
                } else if (nomeComponenteMaiusculo.contains("DISCO")) {
                    limiteMinimoDisco = limiteMinimo;
                    limiteMaximoDisco = limiteMaximo;
                } else if (nomeComponenteMaiusculo.contains("PROCESS")) {
                    limiteMinimoProcessos = limiteMinimo;
                    limiteMaximoProcessos = limiteMaximo;
                }
            }

            dados.setLimiteMinCpu(limiteMinimoCpu);
            dados.setLimiteMaxCpu(limiteMaximoCpu);

            dados.setLimiteMinRam(limiteMinimoRam);
            dados.setLimiteMaxRam(limiteMaximoRam);

            dados.setLimiteMinDisco(limiteMinimoDisco);
            dados.setLimiteMaxDisco(limiteMaximoDisco);

            dados.setLimiteMinProc(limiteMinimoProcessos);
            dados.setLimiteMaxProc(limiteMaximoProcessos);

        } catch (Exception e) {
            System.out.println("BD: Erro ao buscar parâmetros: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (resultadoParametros != null) resultadoParametros.close();
                if (comandoBuscarParametros != null) comandoBuscarParametros.close();
            } catch (Exception e) {
                System.out.println("BD: Erro ao fechar recursos de parâmetros.");
            }
        }
    }

    public static void executarLeituraS3(DashboardData dados,
                                         S3Client s3,
                                         Integer idEmpresaAtual,
                                         String macDoTotem) {

        Map<String, Integer> alertasCpuPorMes = new HashMap<>();
        Map<String, Integer> alertasRamPorMes = new HashMap<>();
        Map<String, Integer> alertasDiscoPorMes = new HashMap<>();
        Map<String, Integer> alertasProcessosPorMes = new HashMap<>();

        Map<LocalDate, Integer> alertasCpuPorDia = new HashMap<>();
        Map<LocalDate, Integer> alertasRamPorDia = new HashMap<>();
        Map<LocalDate, Integer> alertasDiscoPorDia = new HashMap<>();
        Map<LocalDate, Integer> alertasProcessosPorDia = new HashMap<>();

        Integer totalAlertasCpuAno = 0;
        Integer totalAlertasRamAno = 0;
        Integer totalAlertasDiscoAno = 0;
        Integer totalAlertasProcessosAno = 0;

        Integer totalAlertasMuitoPerigosos = 0;

        Integer totalTotensEmEstadoMuitoPerigoso = 0;
        Boolean houveAlertaMuitoPerigosoNesteTotem = false;

        LocalDate dataHoje = LocalDate.now();
        LocalDate dataInicialDos30Dias = dataHoje.minusDays(29);

        try {
            String prefixo = idEmpresaAtual + "/" + macDoTotem + "/";

            ListObjectsV2Request requisicaoListagem = ListObjectsV2Request.builder()
                    .bucket(BUCKET_TRUSTED)
                    .prefix(prefixo)
                    .build();

            ListObjectsV2Response respostaListagem = s3.listObjectsV2(requisicaoListagem);
            List<S3Object> arquivosEncontrados = respostaListagem.contents();

            for (S3Object arquivo : arquivosEncontrados) {

                String caminhoNoBucket = arquivo.key();

                if (!caminhoNoBucket.endsWith("dados.csv")) {
                    continue;
                }

                // Exemplo de caminho: 1/154724027927939/2025-12-01/dados.csv
                String[] partesDoCaminho = caminhoNoBucket.split("/");

                String dataDaPasta = "";

                for (String parteCaminho : partesDoCaminho) {
                    if (parteCaminho.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        dataDaPasta = parteCaminho;
                        break;
                    }
                }

                if (dataDaPasta.isEmpty()) {
                    continue;
                }

                LocalDate dataDoArquivo = LocalDate.parse(dataDaPasta, FORMATO_DATA_PASTA);
                String mesReferencia = dataDaPasta.substring(0, 7); // "AAAA-MM"

                if (!alertasCpuPorMes.containsKey(mesReferencia)) {
                    alertasCpuPorMes.put(mesReferencia, 0);
                }
                if (!alertasRamPorMes.containsKey(mesReferencia)) {
                    alertasRamPorMes.put(mesReferencia, 0);
                }
                if (!alertasDiscoPorMes.containsKey(mesReferencia)) {
                    alertasDiscoPorMes.put(mesReferencia, 0);
                }
                if (!alertasProcessosPorMes.containsKey(mesReferencia)) {
                    alertasProcessosPorMes.put(mesReferencia, 0);
                }

                ResponseInputStream<GetObjectResponse> stream =
                        s3.getObject(GetObjectRequest.builder()
                                .bucket(BUCKET_TRUSTED)
                                .key(caminhoNoBucket)
                                .build());

                BufferedReader leitor = new BufferedReader(
                        new InputStreamReader(stream, StandardCharsets.UTF_8));

                String linhaCsvOriginal;
                Boolean primeiraLinha = true;

                Integer tempoAcimaLimiteCpu = 0;
                Integer tempoAcimaLimiteRam = 0;
                Integer tempoAcimaLimiteDisco = 0;
                Integer tempoAcimaLimiteProcessos = 0;

                while ((linhaCsvOriginal = leitor.readLine()) != null) {

                    if (primeiraLinha) {
                        primeiraLinha = false;
                        continue;
                    }

                    if (linhaCsvOriginal.trim().isEmpty()) {
                        continue;
                    }

                    try {
                        String linhaTratada = normalizarLinhaCsv(linhaCsvOriginal);

                        String[] colunas = linhaTratada.split(",");

                        if (colunas.length < 5) {
                            continue;
                        }

                        Double cpuCapturada = Double.parseDouble(colunas[1].trim());
                        Double ramCapturada = Double.parseDouble(colunas[2].trim());
                        Double discoCapturado = Double.parseDouble(colunas[3].trim());
                        Integer processosCapturados = Integer.parseInt(colunas[4].trim());

                        // CPU
                        if (dados.getLimiteMaxCpu() != null) {
                            if (cpuCapturada > dados.getLimiteMaxCpu()) {
                                tempoAcimaLimiteCpu = tempoAcimaLimiteCpu + 1;
                            } else {
                                if (tempoAcimaLimiteCpu > 0) {
                                    Double minutosEmAlertaCpu = tempoAcimaLimiteCpu / 4.0;

                                    if (minutosEmAlertaCpu >= 15.0) {
                                        totalAlertasCpuAno = totalAlertasCpuAno + 1;

                                        Integer valorMesCpu = alertasCpuPorMes.get(mesReferencia);
                                        if (valorMesCpu == null) {
                                            valorMesCpu = 0;
                                        }
                                        alertasCpuPorMes.put(mesReferencia, valorMesCpu + 1);

                                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                                && !dataDoArquivo.isAfter(dataHoje)) {

                                            Integer valorDiaCpu = alertasCpuPorDia.get(dataDoArquivo);
                                            if (valorDiaCpu == null) {
                                                valorDiaCpu = 0;
                                            }
                                            alertasCpuPorDia.put(dataDoArquivo, valorDiaCpu + 1);
                                        }

                                        if (minutosEmAlertaCpu > 25.0) {
                                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                                            houveAlertaMuitoPerigosoNesteTotem = true;
                                        }
                                    }

                                    tempoAcimaLimiteCpu = 0;
                                }
                            }
                        }

                        // RAM
                        if (dados.getLimiteMaxRam() != null) {
                            if (ramCapturada > dados.getLimiteMaxRam()) {
                                tempoAcimaLimiteRam = tempoAcimaLimiteRam + 1;
                            } else {
                                if (tempoAcimaLimiteRam > 0) {
                                    Double minutosEmAlertaRam = tempoAcimaLimiteRam / 4.0;

                                    if (minutosEmAlertaRam >= 15.0) {
                                        totalAlertasRamAno = totalAlertasRamAno + 1;

                                        Integer valorMesRam = alertasRamPorMes.get(mesReferencia);
                                        if (valorMesRam == null) {
                                            valorMesRam = 0;
                                        }
                                        alertasRamPorMes.put(mesReferencia, valorMesRam + 1);

                                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                                && !dataDoArquivo.isAfter(dataHoje)) {

                                            Integer valorDiaRam = alertasRamPorDia.get(dataDoArquivo);
                                            if (valorDiaRam == null) {
                                                valorDiaRam = 0;
                                            }
                                            alertasRamPorDia.put(dataDoArquivo, valorDiaRam + 1);
                                        }

                                        if (minutosEmAlertaRam > 25.0) {
                                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                                            houveAlertaMuitoPerigosoNesteTotem = true;
                                        }
                                    }

                                    tempoAcimaLimiteRam = 0;
                                }
                            }
                        }

                        // DISCO
                        if (dados.getLimiteMaxDisco() != null) {
                            if (discoCapturado > dados.getLimiteMaxDisco()) {
                                tempoAcimaLimiteDisco = tempoAcimaLimiteDisco + 1;
                            } else {
                                if (tempoAcimaLimiteDisco > 0) {
                                    Double minutosEmAlertaDisco = tempoAcimaLimiteDisco / 4.0;

                                    if (minutosEmAlertaDisco >= 15.0) {
                                        totalAlertasDiscoAno = totalAlertasDiscoAno + 1;

                                        Integer valorMesDisco = alertasDiscoPorMes.get(mesReferencia);
                                        if (valorMesDisco == null) {
                                            valorMesDisco = 0;
                                        }
                                        alertasDiscoPorMes.put(mesReferencia, valorMesDisco + 1);

                                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                                && !dataDoArquivo.isAfter(dataHoje)) {

                                            Integer valorDiaDisco = alertasDiscoPorDia.get(dataDoArquivo);
                                            if (valorDiaDisco == null) {
                                                valorDiaDisco = 0;
                                            }
                                            alertasDiscoPorDia.put(dataDoArquivo, valorDiaDisco + 1);
                                        }

                                        if (minutosEmAlertaDisco > 25.0) {
                                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                                            houveAlertaMuitoPerigosoNesteTotem = true;
                                        }
                                    }

                                    tempoAcimaLimiteDisco = 0;
                                }
                            }
                        }

                        // PROCESSOS
                        if (dados.getLimiteMaxProc() != null) {
                            if (processosCapturados > dados.getLimiteMaxProc()) {
                                tempoAcimaLimiteProcessos = tempoAcimaLimiteProcessos + 1;
                            } else {
                                if (tempoAcimaLimiteProcessos > 0) {
                                    Double minutosEmAlertaProcessos = tempoAcimaLimiteProcessos / 4.0;

                                    if (minutosEmAlertaProcessos >= 15.0) {
                                        totalAlertasProcessosAno = totalAlertasProcessosAno + 1;

                                        Integer valorMesProcessos = alertasProcessosPorMes.get(mesReferencia);
                                        if (valorMesProcessos == null) {
                                            valorMesProcessos = 0;
                                        }
                                        alertasProcessosPorMes.put(mesReferencia, valorMesProcessos + 1);

                                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                                && !dataDoArquivo.isAfter(dataHoje)) {

                                            Integer valorDiaProcessos = alertasProcessosPorDia.get(dataDoArquivo);
                                            if (valorDiaProcessos == null) {
                                                valorDiaProcessos = 0;
                                            }
                                            alertasProcessosPorDia.put(dataDoArquivo, valorDiaProcessos + 1);
                                        }

                                        if (minutosEmAlertaProcessos > 25.0) {
                                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                                            houveAlertaMuitoPerigosoNesteTotem = true;
                                        }
                                    }

                                    tempoAcimaLimiteProcessos = 0;
                                }
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("[S3] Linha inválida: " + linhaCsvOriginal);
                    }
                }

                // Fecha blocos em aberto no final do arquivo (CPU/RAM/DISCO/PROCESSOS)
                if (tempoAcimaLimiteCpu > 0 && dados.getLimiteMaxCpu() != null) {
                    Double minutosEmAlertaCpu = tempoAcimaLimiteCpu / 4.0;

                    if (minutosEmAlertaCpu >= 15.0) {
                        totalAlertasCpuAno = totalAlertasCpuAno + 1;

                        Integer valorMesCpu = alertasCpuPorMes.get(mesReferencia);
                        if (valorMesCpu == null) {
                            valorMesCpu = 0;
                        }
                        alertasCpuPorMes.put(mesReferencia, valorMesCpu + 1);

                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                && !dataDoArquivo.isAfter(dataHoje)) {

                            Integer valorDiaCpu = alertasCpuPorDia.get(dataDoArquivo);
                            if (valorDiaCpu == null) {
                                valorDiaCpu = 0;
                            }
                            alertasCpuPorDia.put(dataDoArquivo, valorDiaCpu + 1);
                        }

                        if (minutosEmAlertaCpu > 25.0) {
                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                            houveAlertaMuitoPerigosoNesteTotem = true;
                        }
                    }
                }

                if (tempoAcimaLimiteRam > 0 && dados.getLimiteMaxRam() != null) {
                    Double minutosEmAlertaRam = tempoAcimaLimiteRam / 4.0;

                    if (minutosEmAlertaRam >= 15.0) {
                        totalAlertasRamAno = totalAlertasRamAno + 1;

                        Integer valorMesRam = alertasRamPorMes.get(mesReferencia);
                        if (valorMesRam == null) {
                            valorMesRam = 0;
                        }
                        alertasRamPorMes.put(mesReferencia, valorMesRam + 1);

                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                && !dataDoArquivo.isAfter(dataHoje)) {

                            Integer valorDiaRam = alertasRamPorDia.get(dataDoArquivo);
                            if (valorDiaRam == null) {
                                valorDiaRam = 0;
                            }
                            alertasRamPorDia.put(dataDoArquivo, valorDiaRam + 1);
                        }

                        if (minutosEmAlertaRam > 25.0) {
                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                            houveAlertaMuitoPerigosoNesteTotem = true;
                        }
                    }
                }

                if (tempoAcimaLimiteDisco > 0 && dados.getLimiteMaxDisco() != null) {
                    Double minutosEmAlertaDisco = tempoAcimaLimiteDisco / 4.0;

                    if (minutosEmAlertaDisco >= 15.0) {
                        totalAlertasDiscoAno = totalAlertasDiscoAno + 1;

                        Integer valorMesDisco = alertasDiscoPorMes.get(mesReferencia);
                        if (valorMesDisco == null) {
                            valorMesDisco = 0;
                        }
                        alertasDiscoPorMes.put(mesReferencia, valorMesDisco + 1);

                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                && !dataDoArquivo.isAfter(dataHoje)) {

                            Integer valorDiaDisco = alertasDiscoPorDia.get(dataDoArquivo);
                            if (valorDiaDisco == null) {
                                valorDiaDisco = 0;
                            }
                            alertasDiscoPorDia.put(dataDoArquivo, valorDiaDisco + 1);
                        }

                        if (minutosEmAlertaDisco > 25.0) {
                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                            houveAlertaMuitoPerigosoNesteTotem = true;
                        }
                    }
                }

                if (tempoAcimaLimiteProcessos > 0 && dados.getLimiteMaxProc() != null) {
                    Double minutosEmAlertaProcessos = tempoAcimaLimiteProcessos / 4.0;

                    if (minutosEmAlertaProcessos >= 15.0) {
                        totalAlertasProcessosAno = totalAlertasProcessosAno + 1;

                        Integer valorMesProcessos = alertasProcessosPorMes.get(mesReferencia);
                        if (valorMesProcessos == null) {
                            valorMesProcessos = 0;
                        }
                        alertasProcessosPorMes.put(mesReferencia, valorMesProcessos + 1);

                        if (!dataDoArquivo.isBefore(dataInicialDos30Dias)
                                && !dataDoArquivo.isAfter(dataHoje)) {

                            Integer valorDiaProcessos = alertasProcessosPorDia.get(dataDoArquivo);
                            if (valorDiaProcessos == null) {
                                valorDiaProcessos = 0;
                            }
                            alertasProcessosPorDia.put(dataDoArquivo, valorDiaProcessos + 1);
                        }

                        if (minutosEmAlertaProcessos > 25.0) {
                            totalAlertasMuitoPerigosos = totalAlertasMuitoPerigosos + 1;
                            houveAlertaMuitoPerigosoNesteTotem = true;
                        }
                    }
                }

                leitor.close();
            }

            if (houveAlertaMuitoPerigosoNesteTotem) {
                totalTotensEmEstadoMuitoPerigoso = 1;
            }

            int totalAlertasCpu30Dias = 0;
            int totalAlertasRam30Dias = 0;
            int totalAlertasDisco30Dias = 0;
            int totalAlertasProcessos30Dias = 0;

            for (Integer valorCpuDia : alertasCpuPorDia.values()) {
                if (valorCpuDia != null) {
                    totalAlertasCpu30Dias = totalAlertasCpu30Dias + valorCpuDia;
                }
            }

            for (Integer valorRamDia : alertasRamPorDia.values()) {
                if (valorRamDia != null) {
                    totalAlertasRam30Dias = totalAlertasRam30Dias + valorRamDia;
                }
            }

            for (Integer valorDiscoDia : alertasDiscoPorDia.values()) {
                if (valorDiscoDia != null) {
                    totalAlertasDisco30Dias = totalAlertasDisco30Dias + valorDiscoDia;
                }
            }

            for (Integer valorProcessosDia : alertasProcessosPorDia.values()) {
                if (valorProcessosDia != null) {
                    totalAlertasProcessos30Dias = totalAlertasProcessos30Dias + valorProcessosDia;
                }
            }

            int totalAlertas30Dias =
                    totalAlertasCpu30Dias
                            + totalAlertasRam30Dias
                            + totalAlertasDisco30Dias
                            + totalAlertasProcessos30Dias;

            dados.setAlertasCpu(totalAlertasCpu30Dias);
            dados.setAlertasRam(totalAlertasRam30Dias);
            dados.setAlertasDisco(totalAlertasDisco30Dias);
            dados.setAlertasProcessos(totalAlertasProcessos30Dias);
            dados.setTotalAlertas(totalAlertas30Dias);

            Integer percentualCritico = 0;
            Integer totalTotensMonitorados = dados.getTotalMonitorado();

            if (totalTotensMonitorados != null && totalTotensMonitorados > 0) {
                Double percentualCalculado =
                        (totalTotensEmEstadoMuitoPerigoso * 100.0) / totalTotensMonitorados;
                percentualCritico = (int) Math.round(percentualCalculado);
            }

            dados.setPercentualCritico(percentualCritico);
            dados.setTotalCriticos(totalTotensEmEstadoMuitoPerigoso);

            montarGraficoAnual(
                    dados,
                    alertasCpuPorMes,
                    alertasRamPorMes,
                    alertasDiscoPorMes,
                    alertasProcessosPorMes
            );

            montarGrafico30Dias(
                    dados,
                    alertasCpuPorDia,
                    alertasRamPorDia,
                    alertasDiscoPorDia,
                    alertasProcessosPorDia,
                    dataInicialDos30Dias,
                    dataHoje
            );

            calcularVariacaoMensal(
                    dados,
                    alertasCpuPorMes,
                    alertasRamPorMes,
                    alertasDiscoPorMes,
                    alertasProcessosPorMes
            );

        } catch (Exception e) {
            System.out.println("ETL: Erro ao ler dados do S3: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void salvarJsonNoS3(DashboardData dados, Integer idEmpresaAtual, String macTotemAtual) {
        try {
            S3Client s3 = S3Client.builder()
                    .region(S3_REGION)
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsSessionCredentials.create(
                                            AWS_ACCESS_KEY,
                                            AWS_SECRET_KEY,
                                            AWS_SESSION_TOKEN
                                    )
                            )
                    )
                    .build();

            ObjectMapper mapper = new ObjectMapper();
            String jsonFinal = mapper.writeValueAsString(dados);

            String caminhoJson = idEmpresaAtual + "/dash/" + macTotemAtual + "/dashboard.json";

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET_CLIENT)
                            .key(caminhoJson)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(jsonFinal)
            );

            System.out.println("[S3] JSON salvo em: " + BUCKET_CLIENT + "/" + caminhoJson);

        } catch (Exception e) {
            System.out.println("[ETL] Erro ao salvar JSON no S3: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void montarGraficoAnual(
            DashboardData dados,
            Map<String, Integer> alertasCpuPorMes,
            Map<String, Integer> alertasRamPorMes,
            Map<String, Integer> alertasDiscoPorMes,
            Map<String, Integer> alertasProcessosPorMes
    ) {

        String[] listaDeNomesDosMeses = {
                "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                "Jul", "Ago", "Set", "Out", "Nov", "Dez"
        };

        List<String> mesesEncontrados = new ArrayList<>(alertasCpuPorMes.keySet());
        Collections.sort(mesesEncontrados);

        for (String mesReferencia : mesesEncontrados) {
            try {
                String[] partesMes = mesReferencia.split("-"); // "2025-01"
                Integer numeroDoMes = Integer.parseInt(partesMes[1]); // 1..12
                String ano = partesMes[0].substring(2);  // "25"

                String nomeDoMes = listaDeNomesDosMeses[numeroDoMes - 1];
                String labelFormatado = nomeDoMes + "/" + ano;

                dados.getMeses().add(labelFormatado);

                Integer totalCpuMes = garantirValorOuZero(alertasCpuPorMes, mesReferencia);
                Integer totalRamMes = garantirValorOuZero(alertasRamPorMes, mesReferencia);
                Integer totalDiscoMes = garantirValorOuZero(alertasDiscoPorMes, mesReferencia);
                Integer totalProcessosMes = garantirValorOuZero(alertasProcessosPorMes, mesReferencia);

                dados.getTotalMensalCpu().add(totalCpuMes);
                dados.getTotalMensalRam().add(totalRamMes);
                dados.getTotalMensalDisco().add(totalDiscoMes);
                dados.getTotalMensalProcessos().add(totalProcessosMes);

            } catch (Exception e) {
                System.out.println("ETL: Erro ao montar gráfico anual para o mês: " + mesReferencia);
            }
        }
    }

    public static void montarGrafico30Dias(
            DashboardData dados,
            Map<LocalDate, Integer> alertasCpuPorDia,
            Map<LocalDate, Integer> alertasRamPorDia,
            Map<LocalDate, Integer> alertasDiscoPorDia,
            Map<LocalDate, Integer> alertasProcessosPorDia,
            LocalDate dataInicialDos30Dias,
            LocalDate dataHoje
    ) {

        LocalDate dataReferencia = dataInicialDos30Dias;
        while (!dataReferencia.isAfter(dataHoje)) {

            if (!alertasCpuPorDia.containsKey(dataReferencia)) {
                alertasCpuPorDia.put(dataReferencia, 0);
            }
            if (!alertasRamPorDia.containsKey(dataReferencia)) {
                alertasRamPorDia.put(dataReferencia, 0);
            }
            if (!alertasDiscoPorDia.containsKey(dataReferencia)) {
                alertasDiscoPorDia.put(dataReferencia, 0);
            }
            if (!alertasProcessosPorDia.containsKey(dataReferencia)) {
                alertasProcessosPorDia.put(dataReferencia, 0);
            }

            dataReferencia = dataReferencia.plusDays(1);
        }

        List<LocalDate> diasEncontrados = new ArrayList<>(alertasCpuPorDia.keySet());
        Collections.sort(diasEncontrados);

        DateTimeFormatter formatoDiaMes = DateTimeFormatter.ofPattern("dd/MM");

        for (LocalDate diaReferencia : diasEncontrados) {

            if (diaReferencia.isBefore(dataInicialDos30Dias) || diaReferencia.isAfter(dataHoje)) {
                continue;
            }

            String labelDia = diaReferencia.format(formatoDiaMes);
            dados.getDias30().add(labelDia);

            Integer totalCpuDia = garantirValorOuZero(alertasCpuPorDia, diaReferencia);
            Integer totalRamDia = garantirValorOuZero(alertasRamPorDia, diaReferencia);
            Integer totalDiscoDia = garantirValorOuZero(alertasDiscoPorDia, diaReferencia);
            Integer totalProcessosDia = garantirValorOuZero(alertasProcessosPorDia, diaReferencia);

            dados.getTotalMensal30Cpu().add(totalCpuDia);
            dados.getTotalMensal30Ram().add(totalRamDia);
            dados.getTotalMensal30Disco().add(totalDiscoDia);
            dados.getTotalMensal30Processos().add(totalProcessosDia);
        }
    }

    public static void calcularVariacaoMensal(
            DashboardData dados,
            Map<String, Integer> alertasCpuPorMes,
            Map<String, Integer> alertasRamPorMes,
            Map<String, Integer> alertasDiscoPorMes,
            Map<String, Integer> alertasProcessosPorMes
    ) {

        if (alertasCpuPorMes.isEmpty()) {
            dados.setVariacaoAlertas(0.0);
            return;
        }

        List<String> mesesEncontrados = new ArrayList<>(alertasCpuPorMes.keySet());
        Collections.sort(mesesEncontrados);

        if (mesesEncontrados.size() == 1) {
            dados.setVariacaoAlertas(0.0);
            return;
        }

        String mesMaisRecente = mesesEncontrados.get(mesesEncontrados.size() - 1);
        String mesAnteriorAoMaisRecente = mesesEncontrados.get(mesesEncontrados.size() - 2);

        Integer totalAlertasMesMaisRecente = 0;
        Integer totalAlertasMesAnterior = 0;

        Integer valorCpuAtual = alertasCpuPorMes.get(mesMaisRecente);
        if (valorCpuAtual == null) {
            valorCpuAtual = 0;
        }
        totalAlertasMesMaisRecente = totalAlertasMesMaisRecente + valorCpuAtual;

        Integer valorRamAtual = alertasRamPorMes.get(mesMaisRecente);
        if (valorRamAtual == null) {
            valorRamAtual = 0;
        }
        totalAlertasMesMaisRecente = totalAlertasMesMaisRecente + valorRamAtual;

        Integer valorDiscoAtual = alertasDiscoPorMes.get(mesMaisRecente);
        if (valorDiscoAtual == null) {
            valorDiscoAtual = 0;
        }
        totalAlertasMesMaisRecente = totalAlertasMesMaisRecente + valorDiscoAtual;

        Integer valorProcessosAtual = alertasProcessosPorMes.get(mesMaisRecente);
        if (valorProcessosAtual == null) {
            valorProcessosAtual = 0;
        }
        totalAlertasMesMaisRecente = totalAlertasMesMaisRecente + valorProcessosAtual;

        Integer valorCpuAnterior = alertasCpuPorMes.get(mesAnteriorAoMaisRecente);
        if (valorCpuAnterior == null) {
            valorCpuAnterior = 0;
        }
        totalAlertasMesAnterior = totalAlertasMesAnterior + valorCpuAnterior;

        Integer valorRamAnterior = alertasRamPorMes.get(mesAnteriorAoMaisRecente);
        if (valorRamAnterior == null) {
            valorRamAnterior = 0;
        }
        totalAlertasMesAnterior = totalAlertasMesAnterior + valorRamAnterior;

        Integer valorDiscoAnterior = alertasDiscoPorMes.get(mesAnteriorAoMaisRecente);
        if (valorDiscoAnterior == null) {
            valorDiscoAnterior = 0;
        }
        totalAlertasMesAnterior = totalAlertasMesAnterior + valorDiscoAnterior;

        Integer valorProcessosAnterior = alertasProcessosPorMes.get(mesAnteriorAoMaisRecente);
        if (valorProcessosAnterior == null) {
            valorProcessosAnterior = 0;
        }
        totalAlertasMesAnterior = totalAlertasMesAnterior + valorProcessosAnterior;

        if (totalAlertasMesAnterior == 0) {
            dados.setVariacaoAlertas(0.0);
        } else {
            Double variacao =
                    ((totalAlertasMesMaisRecente - totalAlertasMesAnterior) * 100.0)
                            / totalAlertasMesAnterior;

            variacao = Math.round(variacao * 10.0) / 10.0;

            dados.setVariacaoAlertas(variacao);
        }
    }

    public static Integer garantirValorOuZero(Map<String, Integer> alertasPorMes, String mesReferencia) {
        Integer valor = alertasPorMes.get(mesReferencia);
        if (valor == null) {
            valor = 0;
        }
        return valor;
    }

    public static Integer garantirValorOuZero(Map<LocalDate, Integer> alertasPorDia, LocalDate diaReferencia) {
        Integer valor = alertasPorDia.get(diaReferencia);
        if (valor == null) {
            valor = 0;
        }
        return valor;
    }

    public static String normalizarLinhaCsv(String linhaOriginal) {

        StringBuilder textoTratado = new StringBuilder();
        boolean estamosDentroDasAspas = false;

        for (int i = 0; i < linhaOriginal.length(); i++) {
            char caractereAtual = linhaOriginal.charAt(i);

            if (caractereAtual == '"') {
                estamosDentroDasAspas = !estamosDentroDasAspas;
            } else if (caractereAtual == ',' && estamosDentroDasAspas) {
                textoTratado.append('.');
            } else {
                textoTratado.append(caractereAtual);
            }
        }

        return textoTratado.toString();
    }
}