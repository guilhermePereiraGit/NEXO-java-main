package school.sptech;

import java.time.DayOfWeek;
import java.time.LocalDate;

public class obterUltimoOuAtualDomingo {

    public static LocalDate obterUltimoOuAtualDomingo() {
        LocalDate hoje = LocalDate.now();

        // Se hoje for domingo, retornar o próprio dia
        if (hoje.getDayOfWeek() == DayOfWeek.SUNDAY) {
            return hoje;
        }

        // Caso contrário, encontrar o último domingo
        return hoje.minusDays(hoje.getDayOfWeek().getValue());
    }

    public static void main(String[] args) {
        System.out.println(obterUltimoOuAtualDomingo());
    }
}
