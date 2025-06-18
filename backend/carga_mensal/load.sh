wget -nc -np -nd -r -A zip https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-05/
cp ./ /tmp/carga_mensal
cd carga_mensal
unzip 'Cnaes.zip' && cat ./*.[A-Z]* > cnaes.csv && rm ./*.[A-Z]*
unzip 'Municipios.zip' && cat ./*.[A-Z]* > municipios.csv && rm ./*.[A-Z]*
unzip 'Naturezas.zip' && cat ./*.[A-Z]* > naturezas.csv && rm ./*.[A-Z]*
unzip 'Paises.zip' && cat ./*.[A-Z]* > paises.csv && rm ./*.[A-Z]*
unzip 'Qualificacoes.zip' && cat ./*.[A-Z]* > qualificacoes.csv && rm ./*.[A-Z]*
unzip 'Simples.zip' && cat ./*.[A-Z]* > simples.csv && rm ./*.[A-Z]*
unzip 'Motivos.zip' && cat ./*.[A-Z]* > motivos.csv && rm ./*.[A-Z]*
unzip 'Empresas*' && cat ./*.[A-Z]* > empresas.csv && rm ./*.[A-Z]*
unzip 'Estabelecimentos*' && cat ./*.[A-Z]* > estabelecimentos.csv && rm ./*.[A-Z]*
unzip 'Socios*' && cat ./*.[A-Z]* > socios.csv && rm ./*.[A-Z]*
rm ./*.zip

