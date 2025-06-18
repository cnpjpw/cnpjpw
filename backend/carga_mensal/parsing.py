import csv
from tqdm import tqdm

def parse_csv_tabela(indices_bool, indices_date, indices_numeric, total_linhas, nome_arq, indice_array=None):
    with open(nome_arq, 'r', encoding='latin-1') as f_in, \
         open(nome_arq + '_tratado.csv', 'w', encoding='latin-1', newline='') as f_out:

        line_reader = csv.reader(f_in, delimiter=';')
        line_writer = csv.writer(f_out, delimiter=';')

        for k, line in enumerate(tqdm(line_reader, total=total_linhas)):
            if indice_array:
                line[indice_array] = '{' + line[indice_array] + '}'
            for i in indices_date:
                #Exemplo: 20250430
                data = line[i]
                if len(data) != 8:
                    line[i] = None
                elif int(data) == 0:
                    line[i] = None
            for i in indices_numeric:
                    line[i] = line[i].replace(',', '.')
            for i in indices_bool:
                val = line[i]
                line[i] = True if val == 'S' else False
            for i in range(len(line)):
                val = line[i]
                if type(val) == str:
                    val = val.replace('\x00', '')
                    line[i] = val
                if val == '':
                    line[i] = None

            line_writer.writerow(line)

if __name__ == '__main__':
    indices_date = []
    indices_numeric = []
    indices_bool = []
    indices_array = []
    total_linhas = 0
    nome_arq = ''
    
    parse_csv_tabela(indices_bool, indices_date, indices_numeric, total_linhas, nome_arq)

