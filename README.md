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

## Infra Usada e Suporte Financeiro

Atualmente essa instância do cnpjpw roda em uma root server da netcup com [8gb de RAM, 4 núcleos e 512GB de disco](https://www.netcup.com/de/server/root-server/rs-1000-g12-ip-12m). Como eu aluguei em uma promoção antes do RAMpocalypse saiu por um preço bem legal, 8,24€/mês, mas [sofrendo os impactos dos aumentos de preço nas memórias no geral e tendo alguns problemas com fornecedores](https://forum.netcup.de/thread/21892-rampocalypse-we-need-to-talk-about-pricing/?postID=262878#post262878) eles tiveram que aumentar um pouco o preço até para usuários antigos, indo para 9,74€. Sinceramente, no momento há 0 necessidade de melhorar hardware. O sistema é bem leve para falar a verdade, a RAM quase não é usada, o disco a gente atualmente usa 165GB com vários índices no banco, com um aumento de empresas bem contido e singelo... Os 4 núcleos conseguem suportar até 10-13 IPS's rodando no topo do rate limit(o que não aconteceu, por enquanto) além da netcup fornecer uma garantia de uptime de 99.9%.

Um usuário me contatou e fez(e ainda faz) algumas doações para o projeto(muito obrigado, Fabrício :D). Para tentar ter mais transparência quanto a isso eu abri um ko-fi. 

 [![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cnpjpw)

 Uma outra forma que você pode ajudar a manter o projeto é usando o [link de afiliado](https://www.netcup.com/de/?ref=267636) da netcup caso você já tenha interesse de obter uma VPS com eles. Usando o link teoricamente eu consigo 10% do que você gastar lá [pelo que eu entendi](https://www.netcup.com/en/helpcenter/documentation/general/affiliate-program).

 Só com as doações feitas por esse usuário até o momoento seria possível manter a infra por pelo menos mais 8 meses(considerando que os preços se mantenham) sem eu precisar tirar 1 centavo do meu bolso(novamente, muito obrigado!). Hoje ainda pago com meu dinheiro e deixo as doações guardadas como uma garantia.

 Mas, gostaria de ressaltar que não tem como eu fornecer [nenhum tipo de garantia](https://github.com/cnpjpw/cnpjpw/issues/6) quanto as atualizações dos dados, somente quanto ao estado do projeto da forma como está atualmente; e também que não há nenhum tipo benefício para doadores.

