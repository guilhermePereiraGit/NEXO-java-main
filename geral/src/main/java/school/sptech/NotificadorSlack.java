package school.sptech;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;import io.github.cdimascio.dotenv.Dotenv;


public class NotificadorSlack {
    public static void enviarMensagem(String message) throws IOException {
        Dotenv dotenv = Dotenv.load();
        String webhookUrl = dotenv.get("SLACK_WEBHOOK");

        String payload = "{\"text\": \"" + message + "\"}";

        URL url = new URL(webhookUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.getBytes());
        }

        if (conn.getResponseCode() != 200) {
            System.out.println("Falha ao enviar mensagem: " + conn.getResponseCode());
        }
    }
}
