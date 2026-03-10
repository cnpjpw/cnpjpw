from etl.parsing import gerar_csvs_utf8
import pathlib
import os
import zipfile
import sys
from pathlib import Path

def teste_extracao_real(tmp_path):
	zip_path = tmp_path / 'A.zip'

	out = tmp_path / 'out'
	out.mkdir(exist_ok=True, parents=True)

	with zipfile.ZipFile(tmp_path / 'A.zip', 'w') as z:
		z.writestr('a.txt', '1,2\n')
	for i in range(10):
		with zipfile.ZipFile(tmp_path / f'B{i}.zip', 'w') as z:
			z.writestr(f'nomemaluco{i}.txt', '1,2\n')

	gerar_csvs_utf8(tmp_path, out, nao_numerados=['A'], numerados=['B'])

	assert (out / 'A.csv').exists()
	assert (out / 'B.csv').exists()
	with open(out / 'A.csv', 'r') as f1, \
	     open(out / 'B.csv', 'r') as f2:
		assert len(f1.readlines()) == 1
		assert len(f2.readlines()) == 10
	
	(out / 'A.csv').unlink()
	(out / 'B.csv').unlink()
	(tmp_path / 'A.zip').unlink()
	for i in range(10):
		(tmp_path / f'B{i}.zip').unlink()


teste_extracao_real(pathlib.Path(__file__).parent / 'tmp')
