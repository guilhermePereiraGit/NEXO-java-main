package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.S3Event;

public class Main{

    public static void main(String[] args) {
        Etl etl = new Etl();

        etl.buscarQuantidadeDeTecnicoPorRegiao();
    }
}
