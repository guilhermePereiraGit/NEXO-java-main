package sptech.nexo;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ETL {
    //Definição de Atributos para Contato com ETL
    private static String BUCKET_TRUSTED = "bucket-trusted-nexo-silva";
    private static String BUCKET_CLIENT = "bucket-client-nexo-silva";
    private static Region S3_REGION = Region.US_EAST_1;
    private static S3Client s3Client;
    static DateTimeFormatter FORMATO_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    static DateTimeFormatter FORMATO_SAIDA   = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
    }

    private static List<Totem> pegarTotens(JdbcTemplate conexaoBanco){
        List<Totem> totens = conexaoBanco.query("""
                select t.idTotem,t.numMac,m.nome AS nomeModelo,r.nome AS nomeRegiao,r.sigla
                from totem as t\s
                inner join modelo as m on t.fkModelo = m.idModelo
                inner join endereco as e on t.fkEndereco = e.idEndereco
                inner join regiao as r on e.fkRegiao = r.idRegiao; 
                """, new TotemMapper());
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

        for (Totem t : pegarTotens(jdbcTemplate)){
            System.out.println(t);
            tratarDadosClient(t.getNumMac(), empresas.pegarTotalEmpresas());
        }
    }
}
