"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby


#
# Escriba la funcion que  genere n copias de los archivos de texto en la
# carpeta files/raw en la carpeta files/input. El nombre de los archivos
# generados debe ser el mismo que el de los archivos originales, pero con
# un sufijo que indique el número de copia. Por ejemplo, si el archivo
# original se llama text0.txt, el archivo generado se llamará text0_1.txt,
# text0_2.txt, etc.
#
def copy_raw_files_to_input_folder(n):
    raw_files = glob.glob('files/raw/*.txt')
    for file in raw_files:
        base_name = os.path.basename(file)
        for i in range(1, n + 1):
            new_file_name = f"files/input/{base_name.replace('.txt', f'_{i}.txt')}"
            with open(file, 'r') as src_file:
                with open(new_file_name, 'w') as dest_file:
                    dest_file.write(src_file.read())


#
# Escriba la función load_input que recive como parámetro un folder y retorna
# una lista de tuplas donde el primer elemento de cada tupla es el nombre del
# archivo y el segundo es una línea del archivo. La función convierte a tuplas
# todas las lineas de cada uno de los archivos. La función es genérica y debe
# leer todos los archivos de folder entregado como parámetro.
#
# Por ejemplo:
#   [
#     ('text0'.txt', 'Analytics is the discovery, inter ...'),
#     ('text0'.txt', 'in data. Especially valuable in ar...').
#     ...
#     ('text2.txt'. 'hypotheses.')
#   ]
#
def load_input(input_directory):
    data = []
    for filename in os.listdir(input_directory):
        if filename.endswith('.txt'):
            with open(os.path.join(input_directory, filename), 'r') as f:
                for line in f:
                    data.append((filename, line.strip()))
    return data



#
# Escriba la función line_preprocessing que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). Esta función
# realiza el preprocesamiento de las líneas de texto,
#
import string


def line_preprocessing(sequence):
    processed = []
    for filename, line in sequence:
        # Eliminar puntuación y convertir a minúsculas
        line = line.translate(str.maketrans('', '', string.punctuation)).lower()
        words = line.split()
        for word in words:
            processed.append((word, 1))
    return processed



#
# Escriba una función llamada maper que recibe una lista de tuplas de la
# función anterior y retorna una lista de tuplas (clave, valor). En este caso,
# la clave es cada palabra y el valor es 1, puesto que se está realizando un
# conteo.
#
#   [
#     ('Analytics', 1),
#     ('is', 1),
#     ...
#   ]
#
def mapper(sequence):
    return sequence



#
# Escriba la función shuffle_and_sort que recibe la lista de tuplas entregada
# por el mapper, y retorna una lista con el mismo contenido ordenado por la
# clave.
#
#   [
#     ('Analytics', 1),
#     ('Analytics', 1),
#     ...
#   ]
#
def shuffle_and_sort(sequence):
    return sorted(sequence, key=lambda x: x[0])



#
# Escriba la función reducer, la cual recibe el resultado de shuffle_and_sort y
# reduce los valores asociados a cada clave sumandolos. Como resultado, por
# ejemplo, la reducción indica cuantas veces aparece la palabra analytics en el
# texto.
#
from itertools import groupby


def reducer(sequence):
    sorted_sequence = sorted(sequence, key=lambda x: x[0])
    reduced = []
    for key, group in groupby(sorted_sequence, key=lambda x: x[0]):
        total = sum(val for key, val in group)
        reduced.append((key, total))
    return reduced



#
# Escriba la función create_ouptput_directory que recibe un nombre de
# directorio y lo crea. Si el directorio existe, lo borra
#
import shutil


def create_output_directory(output_directory):
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)



#
# Escriba la función save_output, la cual almacena en un archivo de texto
# llamado part-00000 el resultado del reducer. El archivo debe ser guardado en
# el directorio entregado como parámetro, y que se creo en el paso anterior.
# Adicionalmente, el archivo debe contener una tupla por línea, donde el primer
# elemento es la clave y el segundo el valor. Los elementos de la tupla están
# separados por un tabulador.
#
def save_output(output_directory, sequence):
    with open(os.path.join(output_directory, 'part-00000'), 'w') as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")



#
# La siguiente función crea un archivo llamado _SUCCESS en el directorio
# entregado como parámetro.
#
def create_marker(output_directory):
    with open(os.path.join(output_directory, '_SUCCESS'), 'w') as f:
        f.write('Job completed successfully.\n')



#
# Escriba la función job, la cual orquesta las funciones anteriores.
#
def run_job(input_directory, output_directory):
    create_output_directory(output_directory)
    
    # Cargar datos
    input_data = load_input(input_directory)
    
    # Preprocesar líneas
    preprocessed_data = line_preprocessing(input_data)
    
    # Mapear
    mapped_data = mapper(preprocessed_data)
    
    # Ordenar y agrupar
    sorted_data = shuffle_and_sort(mapped_data)
    
    # Reducir
    reduced_data = reducer(sorted_data)
    
    # Guardar resultado
    save_output(output_directory, reduced_data)
    
    # Crear marcador de éxito
    create_marker(output_directory)



if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
