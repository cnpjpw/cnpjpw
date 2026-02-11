# O que é o cnpj.pw?

O [cnpj.pw](https://cnpj.pw) é um projeto que visa disponibilizar de forma estruturada, pública e open-source por meio de uma [API](https://api.cnpj.pw/docs)  acesso aos dados públicos do CNPJ. 
Utilizamos a [base disponibilizada mensalmente pela receita](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/) como nossa fonte primária dos dados.

Atualmente já existe no mercado outras aplicações que realizam esse trabalho: 
- Projetos open source como o [minhareceita](https://github.com/cuducos/minha-receita) e o [CNPJ Data Pipeline](https://github.com/caiopizzol/cnpj-data-pipeline)
- Soluções comerciais como o [cnpj.biz](http://cnpj.biz), [casadosdados](http://casadosdados.com.br) e [cnpja](http://cnpja.com) 
- Produtos oficiais da própria SERPRO(serviço de processamento de dados federal) como o [b-cadastros](https://loja.serpro.gov.br/b-cadastros) e o [Consulta CNPJ](https://loja.serpro.gov.br/consultacnpj)

O que o [cnpj.pw](https://cnpj.pw) tenta fazer é juntar as características mais interessantes dessas aplicações:
- Open-source.
- Filtros para busca detalhada(filtragem por data de abertura, CPF de sócios, cnaes, cidades, estados, capital social, razão social…)
- Complementar a base da RFB com web scraping(todos os dados obtidos com a execução do scraper são armazenadas no [archive](https://archive.cnpj.pw))
- Acessível programaticamente de forma simples(API pública)
