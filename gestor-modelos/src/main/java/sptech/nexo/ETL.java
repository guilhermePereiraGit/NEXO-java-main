package sptech.nexo;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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
        //Para navegar por todas as empresas
        for (int i = 1; i <= totalEmpresas; i++) {
            //Pegar últimos 7 Dias:
            //Esse código vai navegar convertendo a data nos diretórios dos buckets de string para data e pegando seus arquivos
            for (int j = 0; j < 8; j++) {
                //Este minusDay substrai dias de uma data, essencial para neste caso, pegar os arquivos de hoje até 7 dias atrás pasando por todos os dias
                LocalDate hoje = LocalDate.now().minusDays(j);
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

                    //Temporário (Isso foi usando IA, mas é só um teste para eu saber se ele está pegando os arquivos)
                    byte[] bytes = s3objectStream.readAllBytes();
                    System.out.println("✔ Arquivo encontrado (" + bytes.length + " bytes) → " + caminhoArquivos);

                    //A partir daqui eu tenho que filtrar as informações pelos dados relevantes e transformar em JSON
                    //Além disso, preciso criar um objeto que servirá como molde para todos os objetos à serem colocados no JSON
                    //Este objeto deverá conter o UPTIME - Milisegundos referentes à cada dia até os 7 Dias de distância
                    //Além de categorizar pelos alertas pegando os parâmetros para cada objeto

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
        }
    }
}
