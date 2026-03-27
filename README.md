# O que é o cnpj.pw?

O [cnpj.pw](https://cnpj.pw) é um projeto que visa disponibilizar de forma estruturada, pública e open-source por meio de uma [API](https://api.cnpj.pw/docs)  acesso aos dados públicos do CNPJ. 
Utilizamos a [base disponibilizada mensalmente pela receita](https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9) como nossa fonte primária dos dados.

Atualmente há outras aplicações que realizam esse trabalho: 
- Projetos open source como o [minhareceita](https://github.com/cuducos/minha-receita) e o [CNPJ Data Pipeline](https://github.com/caiopizzol/cnpj-data-pipeline)
- Soluções comerciais como o [cnpj.biz](http://cnpj.biz), [casadosdados](http://casadosdados.com.br) e [cnpja](http://cnpja.com) 
- Produtos oficiais da própria SERPRO(serviço de processamento de dados federal) como o [b-cadastros](https://loja.serpro.gov.br/b-cadastros) e o [Consulta CNPJ](https://loja.serpro.gov.br/consultacnpj)

O que o [cnpj.pw](https://cnpj.pw) tenta fazer é juntar as características mais interessantes dessas aplicações:
- Open-source.
- Filtros para busca detalhada(filtragem por data de abertura, CPF de sócios, cnaes, cidades, estados, capital social, razão social…)
- Complementar a base da RFB com web scraping(todos os dados obtidos com a execução do scraper são armazenadas no [archive](https://archive.cnpj.pw))
- Acessível programaticamente de forma simples(API pública)

## Acesso a API

Qualquer IP pode acessar essa instância da API com rate limit de 10req/IP/s(ao ultrapassar esse limite a API começará a retornar 503) e sem necessidade de login atualmente. Basta acessar https://api.cnpj.pw/docs e usar o(s) endpoint(s) que melhor se adeque(m) ao seu caso de uso. Se você tem algum caso de uso que acredita que a API poderia suprir melhor e que seria útil para mais pessoas, abra uma issue.

## Acesso ao archive

O archive do cnpjpw é onde fica salvo todas as runs do scraper usado para tentar remediar um pouco o gap entre as atualizações oficiais da RFB. O código do scraper não é público e nem será por alguns motivos que eu descrevo um pouco melhor [aqui](https://github.com/cnpjpw/cnpjpw/issues/2). Ainda assim, o resultados das coletas ficam todos salvos em https://archive.cnpj.pw antes mesmo de ir para o banco de dados, no mesmo formato disponibilizado pela receita(com algumas informações incompletas que só irão ser preenchidas na API com a atualização mensal da receita). É importante observar também que não há garantia de continuidade da atualização não mensal, então não conte com isso como garantido. Aqui um [exemplo de client](https://github.com/cnpjpw/client_archive) usando as atualizações do archive para montar um sqlite local.

## Contribuições

Eu ainda não tenho um guia para contribuições(nem para instalação local, nem para MUITA coisa...). O que tento fazer atualmente é colocar nas issues o que eu planejo fazer com base no que eu acho importante para o projeto, então talvez seja uma boa ideia dar uma olhada lá;

## Contato

Você pode entrar em contato abrindo uma issue ou mandando um email para contato@cnpj.pw

 [![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cnpjpw)

