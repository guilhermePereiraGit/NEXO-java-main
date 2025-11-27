package sptech.nexo;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ETL {
    //Definição de Atributos para Contato com ETL
    private static String BUCKET_TRUSTED = "bucket-trusted-nexo-silva";
    private static String BUCKET_CLIENT = "bucket-client-nexo-silva";
    private static Region S3_REGION = Region.US_EAST_1;
    private static S3Client s3Client;

    //Este bloco static é executado quando o código em questão é carregado
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

    //Métodos para Captura de Dados do Trusted e Tratamento
    private static void tratarDadosClient(String macOrigem, Integer totalEmpresas){
        Scanner entrada = null;
        Boolean falhou = false;

        //Para retirar possíveis espaços em branco que podem surgir
        macOrigem = macOrigem.trim();

        System.out.println(totalEmpresas);
        //Para navegar por todas as empresas
        for (int i = 1; i <= totalEmpresas; i++) {
            String arqJson = "[";
            //Pegar últimos 7 Dias:
            //Esse código vai navegar convertendo a data nos diretórios dos buckets de string para data e pegando seus arquivos
            for (int j = 0; j < 7; j++) {
                //Este minusDay substrai dias de uma data, essencial para neste caso, pegar os arquivos de hoje até 7 dias atrás pasando por todos os dias
                LocalDate hoje = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(j);
                String dataFormatada = hoje.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));

                //Para ele buscar os arquivos corretamente, eu vou criar uma variável key passando o caminho com base
                //na data seguindo o modelo de dia à dia
                String caminhoArquivos = "empresa-"+i+"/"+macOrigem+"/"+dataFormatada+"/dados.csv";

                //Agora basta tentar ler o arquivo com base no caminho construído
                try{
                    //Tentando Ler Trusted
                    GetObjectRequest getRequest = GetObjectRequest.builder()
                            .bucket(BUCKET_TRUSTED)
                            .key(caminhoArquivos)
                            .build();

                    ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
                    entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

                    //A partir daqui eu tenho que filtrar as informações pelos dados relevantes e transformar em JSON
                    //Além disso, preciso criar um objeto que servirá como molde para todos os objetos à serem colocados no JSON
                    //Este objeto deverá conter o UPTIME - Milisegundos referentes à cada dia até os 7 Dias de distância
                    //Além de categorizar pelos alertas pegando os parâmetros para cada objeto

                    //Leitura do CSV
                    Boolean cabecalho = true;
                    Map<String, List<String>> linhasPorMac = new HashMap<>();
                    String headerLine = null;
                    String totensJson = "";
                    String[] ultimaLinhaCSV = null;

                    while(entrada.hasNextLine()){
                        String linha = entrada.nextLine();
                        String[] valores = linha.split(",", -1);

                        if (cabecalho){
                            cabecalho = false;
                        }else if (valores.length >= 9){
                            ultimaLinhaCSV = valores;
                        }else{
                            System.out.println("A Linha não Possui o Número de Valores Suficiente");
                        }
                    }
                    if (ultimaLinhaCSV != null){
                        //Convertendo Uptime de milisegundos para horas
                        Double uptime = Double.parseDouble(ultimaLinhaCSV[5]) / 3600000;

                        //Calculando o Downtime
                        Double downtime1Dia = uptime - 24;
                        Double downtime2Dia = uptime - (24*2);
                        Double downtime3Dia = uptime - (24*3);
                        Double downtime4Dia = uptime - (24*4);
                        Double downtime5Dia = uptime - (24*5);
                        Double downtime6Dia = uptime - (24*6);
                        Double downtime7Dia = uptime - (24*7);

                        //Adicionando o Json do Totem
                        totensJson += """
                                { "modelo": "%s","data": "%s", "downtime1Dia": %f, "downtime2Dia": %f, "downtime3Dia": %f, "downtime4Dia": %f, "downtime5Dia": %f , "downtime6Dia": %f, "downtime7Dia": %f},
                                """.formatted(ultimaLinhaCSV[7],dataFormatada,downtime1Dia,downtime2Dia,downtime3Dia,downtime4Dia,downtime5Dia,downtime6Dia,downtime7Dia);
                    }

                    arqJson += totensJson;

                }catch (S3Exception e){
                    System.out.println("Arquivo TRUSTED não existe para o MAC " + macOrigem + ": " + e.getMessage());
                    falhou = true;
                } catch (Exception erro) {
                    System.out.println("Erro ao acessar S3 TRUSTED!");
                    erro.printStackTrace();
                    falhou = true;
                } finally {
                    if (entrada != null) entrada.close();
                    if (falhou) System.exit(1);
                }
            }

            //Para remover a última vírgula do último objeto colocado no json
            arqJson = arqJson.replaceAll(",\\s*$", "");
            //Fechando o arqJson para realmente virar um Json
            arqJson += "]";

            //Enviar para o Client
            try {
                String enviarClient = "empresa-"+i+"/"+macOrigem+"/downtime.json";
                PutObjectRequest putRequest = PutObjectRequest.builder()
                        .bucket(BUCKET_CLIENT)
                        .key(enviarClient)
                        .contentType("application/json")
                        .build();
                s3Client.putObject(putRequest, RequestBody.fromString(arqJson));
            }catch (Exception e){
                System.out.println("Erro ao Acessar S3 Client");
            }
        }
    }

    public static void gerarAlertas(Totem totem, Connection conexaoBanco,Integer totalEmpresas){
        Scanner entrada = null;
        Boolean falhou = false;

        //Para retirar possíveis espaços em branco que podem surgir
        String macOrigem = totem.getNumMac();
        macOrigem = macOrigem.trim();

        System.out.println(totalEmpresas);
        //Para navegar por todas as empresas
        for (int i = 1; i <= totalEmpresas; i++) {
            //Pegar últimos 7 Dias:
            //Esse código vai navegar convertendo a data nos diretórios dos buckets de string para data e pegando seus arquivos
            for (int j = 0; j < 7; j++) {
                //Este minusDay substrai dias de uma data, essencial para neste caso, pegar os arquivos de hoje até 7 dias atrás pasando por todos os dias
                LocalDate hoje = LocalDate.now(ZoneId.of("America/Sao_Paulo")).minusDays(j);
                String dataFormatada = hoje.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));

                //Para ele buscar os arquivos corretamente, eu vou criar uma variável key passando o caminho com base
                //na data seguindo o modelo de dia à dia
                String caminhoArquivos = "empresa-"+i+"/"+macOrigem+"/"+dataFormatada+"/dados.csv";

                //Agora basta tentar ler o arquivo com base no caminho construído
                try{
                    //Tentando Ler Trusted
                    GetObjectRequest getRequest = GetObjectRequest.builder()
                            .bucket(BUCKET_TRUSTED)
                            .key(caminhoArquivos)
                            .build();

                    ResponseInputStream<GetObjectResponse> s3objectStream = s3Client.getObject(getRequest);
                    entrada = new Scanner(new InputStreamReader(s3objectStream, StandardCharsets.UTF_8));

                    //A partir daqui eu tenho que filtrar as informações pelos dados relevantes e transformar em JSON
                    //Além disso, preciso criar um objeto que servirá como molde para todos os objetos à serem colocados no JSON
                    //Este objeto deverá conter o UPTIME - Milisegundos referentes à cada dia até os 7 Dias de distância
                    //Além de categorizar pelos alertas pegando os parâmetros para cada objeto

                    //Leitura do CSV
                    Boolean cabecalho = true;
                    Map<String, List<String>> linhasPorMac = new HashMap<>();
                    String headerLine = null;
                    Integer limiteCPU = null;
                    Integer limiteRAM = null;
                    Integer limiteDISCO = null;
                    Integer limitePROC = null;

                    //for para atribuir os limites
                    for (Parametro p : totem.getModelo().getParametros()){
                        String parametroAtual = p.getTipoParametro().getComponente().toLowerCase();
                        if (p.getLimiteMax() != null){
                            if (parametroAtual.contains("cpu")){
                                limiteCPU = p.getLimiteMax();
                            }
                            if (parametroAtual.contains("ram")){
                                limiteRAM = p.getLimiteMax();
                            }
                            if (parametroAtual.contains("disco")){
                                limiteDISCO = p.getLimiteMax();
                            }
                            if (parametroAtual.contains("processos")){
                                limitePROC = p.getLimiteMax();
                            }
                        }else{
                            System.out.println("LIMITES NÃO ENCONTRADOS");
                        }
                    }

                    List<String> passou = new ArrayList<>();
                    Boolean alertaCPU = false, alertaRAM = false, alertaDISCO = false, alertaPROC = false;
                    Integer alertasCPU = 0, alertasRAM = 0, alertasDISCO = 0, alertasPROC = 0;
                    String grauCPU = "",grauRAM = "",grauDISCO = "",grauPROC = "";
                    Alerta aCPU = new Alerta();
                    Alerta aRAM = new Alerta();
                    Alerta aDISCO = new Alerta();
                    Alerta aPROC = new Alerta();

                    while(entrada.hasNextLine()){
                        String linha = entrada.nextLine();
                        String[] valores = linha.split(",", -1);

                        if (cabecalho){
                            String tTimestamp = valores[0];
                            String tCpu = valores[1];
                            String tRam = valores[2];
                            String tDisco = valores[3];
                            String tProcessos = valores[4];
                            String tUptime = valores[5];
                            String tMac = valores[6];
                            String tModelo = valores[7];
                            String tIdEmpresa = valores[8];
                            cabecalho = false;

                        }else if (valores.length >= 9){
                            //GerarAlertas
                            Integer totalPontuacaoAlerta = 0;
                            Integer pontuacaoAtual = 0;

                            //Os alertas são gerados da seguinte forma, a leitura do Python é feita à cada 10 segundos
                            //Com isso, foram definidas algumas métricas para delimitar a criticidade, sendo feita por
                            //uma combinação de 2 fatores, sendo eles, à partir das medições dos componentes, e o que
                            //será utilizado na geração de alertas e aparições para os usuários, que é à partir do tempo
                            //Com isso, segue os parâmetros de alerta de tempo
                            //10 Segundos por Coleta
                            //360 Linhas = 3600 segundos = 1h (Tempo de Execução da Lambda)
                            //90 Linhas = 900 segundos = 15 min (Atenção) = 1 ponto
                            //180 Linhas = 1800 segundos = 30 min (Perigoso) = 2 pontos
                            //Acima disso = Muito Perigoso = 3 pontos

                            //Para Criticidade dos Componentes, seguimos baseando no parâmetro estipulado pelo fabricante
                            //tirando uma média daqueles que ultrapassem a linha de alerta, em seguida, uma média daqueles
                            //que ultrapassam essa média, tendo por fim, as 3 criticidades catalogadas
                            //Dadas todas as explicações de métricas, vamos para o código:

                            //Valores de Cada Componente
                            Double vCpu = Double.parseDouble(valores[1]);
                            Double vRam = Double.parseDouble(valores[2]);
                            Double vDisco = Double.parseDouble(valores[3]);
                            Double vProcesso = Double.parseDouble(valores[4]);
                            String vModelo = valores[7];
                            String vEmpresa = valores[8];

                            //Para CPU
                            if (vCpu > limiteCPU*1.5){
                                alertasCPU += 3;
                            }else if (vCpu > limiteCPU*1.25){
                                alertasCPU += 2;
                            }else if(vCpu > limiteCPU){
                                alertasCPU += 1;
                            }

                            //Para RAM
                            if (vRam > limiteRAM*1.5){
                                alertasRAM += 3;
                            }else if (vRam > limiteRAM*1.25){
                                alertasRAM += 2;
                            }else if(vRam > limiteRAM){
                                alertasRAM += 1;
                            }

                            //Para DISCO
                            if (vDisco > limiteDISCO*1.5){
                                alertasDISCO += 3;
                            }else if (vDisco > limiteDISCO*1.25){
                                alertasDISCO += 2;
                            }else if(vDisco > limiteDISCO){
                                alertasDISCO += 1;
                            }

                            //Para PROCESSOS
                            if (vProcesso > limitePROC*1.5){
                                alertasPROC += 3;
                            }else if (vProcesso > limitePROC*1.25){
                                alertasPROC += 2;
                            }else if(vProcesso > limitePROC){
                                alertasPROC += 1;
                            }

                            //Definição de Alertas
                            //CPU
                            if (alertasCPU > 360){
                                aCPU.setComponente("CPU");
                                aCPU.setGrau("Muito Perigoso");
                                aCPU.setModelo(totem.getModelo().getNome());
                            }else if(alertasCPU > 180){
                                aCPU.setComponente("CPU");
                                aCPU.setGrau("Perigoso");
                                aCPU.setModelo(totem.getModelo().getNome());
                            }else if(alertasCPU > 90){
                                aCPU.setComponente("CPU");
                                aCPU.setGrau("Atenção");
                                aCPU.setModelo(totem.getModelo().getNome());
                            }

                            //RAM
                            if (alertasRAM > 360){
                                aRAM.setComponente("RAM");
                                aRAM.setGrau("Muito Perigoso");
                                aRAM.setModelo(totem.getModelo().getNome());
                            }else if(alertasRAM > 180){
                                aRAM.setComponente("RAM");
                                aRAM.setGrau("Perigoso");
                                aRAM.setModelo(totem.getModelo().getNome());
                            }else if(alertasRAM > 90){
                                aRAM.setComponente("RAM");
                                aRAM.setGrau("Atenção");
                                aRAM.setModelo(totem.getModelo().getNome());
                            }

                            //DISCO
                            if (alertasDISCO > 360){
                                aDISCO.setComponente("DISCO");
                                aDISCO.setGrau("Muito Perigoso");
                                aDISCO.setModelo(totem.getModelo().getNome());
                            }else if(alertasDISCO > 180){
                                aDISCO.setComponente("DISCO");
                                aDISCO.setGrau("Perigoso");
                                aDISCO.setModelo(totem.getModelo().getNome());
                            }else if(alertasDISCO > 90){
                                aDISCO.setComponente("DISCO");
                                aDISCO.setGrau("Atenção");
                                aDISCO.setModelo(totem.getModelo().getNome());
                            }

                            //PROCESSOS
                            if (alertasPROC > 360){
                                aPROC.setComponente("PROCESSOS");
                                aPROC.setGrau("Muito Perigoso");
                                aPROC.setModelo(totem.getModelo().getNome());
                            }else if(alertasPROC > 180){
                                aPROC.setComponente("PROCESSOS");
                                aPROC.setGrau("Perigoso");
                                aPROC.setModelo(totem.getModelo().getNome());
                            }else if(alertasPROC > 90){
                                aPROC.setComponente("PROCESSOS");
                                aPROC.setGrau("Atenção");
                                aPROC.setModelo(totem.getModelo().getNome());
                            }

                        }else{
                            System.out.println("A Linha não Possui o Número de Valores Suficiente");
                        }
                    }

                    List<Alerta> alertas = new ArrayList<>();
                    if (!(aCPU.getGrau() == null)){
                        alertas.add(aCPU);
                    }
                    if (!(aRAM.getGrau() == null)){
                        alertas.add(aRAM);
                    }
                    if (!(aDISCO.getGrau() == null)){
                        alertas.add(aDISCO);
                    }
                    if (!(aPROC.getGrau() == null)){
                        alertas.add(aPROC);
                    }

                    String alertasArq = "[";
                    for (Alerta a : alertas){
                        alertasArq += """
                                { "modelo": "%s","componente":"%s", "grau":"%s"},""".formatted(
                                a.getModelo(), a.getComponente(), a.getGrau());
                    }

                    alertasArq = alertasArq.replaceAll(",\\s*$", "");
                    alertasArq += "]";

                    //Enviar para o Client
                    try {
                        String enviarClient = "empresa-"+i+"/"+macOrigem+"/alertas-"+dataFormatada+".json";
                        PutObjectRequest putRequest = PutObjectRequest.builder()
                                .bucket(BUCKET_CLIENT)
                                .key(enviarClient)
                                .contentType("application/json")
                                .build();
                        s3Client.putObject(putRequest, RequestBody.fromString(alertasArq));
                    }catch (Exception e){
                        System.out.println("Erro ao Acessar S3 Client");
                    }

                }catch (S3Exception e){
                    System.out.println("Arquivo TRUSTED não existe para o MAC " + macOrigem + ": " + e.getMessage());
                    falhou = true;
                } catch (Exception erro) {
                    System.out.println("Erro ao acessar S3 TRUSTED!");
                    erro.printStackTrace();
                    falhou = true;
                } finally {
                    if (entrada != null) entrada.close();
                    if (falhou) System.exit(1);
                }
            }
        }
    }

    private static List<Totem> pegarTotens(JdbcTemplate conexaoBanco){
        List<Totem> totens = conexaoBanco.query("""
                select t.idTotem,t.numMac,m.nome AS nomeModelo,r.nome AS nomeRegiao,r.sigla,m.idModelo
                from totem as t\s
                inner join modelo as m on t.fkModelo = m.idModelo
                inner join endereco as e on t.fkEndereco = e.idEndereco
                inner join regiao as r on e.fkRegiao = r.idRegiao; 
                """, new TotemMapper());

        //Adicionar aqui coleta de parâmetros para cada modelo
        for (Totem t : totens){
            Modelo modelo =t.getModelo();
            if (modelo != null){
                List<Parametro> parametros = conexaoBanco.query("""
                        SELECT p.idParametro, p.limiteMin, p.limiteMax, comp.idComponente AS idTipoParametro, comp.nome AS componente, comp.status AS status
                        FROM parametro p
                        INNER JOIN componente comp ON p.fkComponente = comp.idComponente
                        WHERE p.fkModelo = ?
                        """,new ParametroMapper(),modelo.getIdModelo());
                modelo.setParametros(parametros);
            }
        }
        return totens;
    }

    //Main para execução e teste da ETL
    public static void main(String[] args) {
        //Cria a conexão com banco e envia a mesma para pegar todas as empresas cadastradas
        Connection conexaoBanco = new Connection();
        Empresas empresas = new Empresas(conexaoBanco);

        //Aproveitamento de Código
        //Para fins de praticidade e reaproveitamento de código, utilizei as seguintes classes da ETL principal:
        //Modelo, ModeloMapper, Parametro, ParametroMapper, TipoParametro, Totem e TotemMapper
        //Em suma, as classes Mapper fazem a conversão de uma linha do CSV em um objeto para um lista, que irei fazer em uma função própria
        //As classes que não são mapper são justamente as classes objeto que irão receber a conversão

        JdbcTemplate jdbcTemplate = new JdbcTemplate(conexaoBanco.getDataSource());

        for (Totem t : pegarTotens(jdbcTemplate)) {
            tratarDadosClient(t.getNumMac(), empresas.pegarTotalEmpresas());
            gerarAlertas(t, conexaoBanco ,empresas.pegarTotalEmpresas());
        }
    }
}
