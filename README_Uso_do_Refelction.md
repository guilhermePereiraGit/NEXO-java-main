## Coisas que não vimos que tem no código:
**Reflection**: o código usa `java.lang.reflect` para executar métodos pelo nome.  
Exemplo: `executar("getUsoCPU")` chama o método `getUsoCPU()` automaticamente.  
Isso permite escolher qual informação pegar (CPU, RAM ou Disco) de forma dinâmica,  
reduzindo o código e deixando mais limpo.