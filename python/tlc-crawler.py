import requests, os, subprocess, pprint
from bs4 import BeautifulSoup
from os import path
import pyarrow.parquet as pq


def list_parquet_links(url:str, filter_funct):

  response = requests.get(url)
  soup = BeautifulSoup(response.text, 'html.parser')

  links = soup.find_all('a', href=True)
  # data_links = [a['href'] for a in links if any(ext in a['href'] for ext in ['.csv', '.parquet'])]
  data_links = [a['href'] for a in links if '.parquet' in a['href']]
  # data_links = [ l for l in data_links if "fhvhv_tripdata" in l ]
  data_links = filter( filter_funct, data_links )
  data_links = list(map(lambda s: s.strip(), data_links))

  print(f"Se encontraron {len(data_links)} archivos Parquet")

  return data_links

def download_files(data_links: list, dest_folder: str):

  
  # Download directory
  os.makedirs( dest_folder, exist_ok=True)

  print(f"Downloading {len(data_links)} files")

  for (idx, link) in zip(range(1,len(data_links)+1),data_links):

    filename = path.join(dest_folder, link.split("/")[-1])
    print(f"{idx} Downloading {filename}...")
    r = requests.get(link)
    with open(filename, 'wb') as f:
      f.write(r.content)

def check_parquet_files(check_path:str):

  file_list = []

  for f in os.listdir(check_path):

    try:

      # Abrir archivo Parquet
      parquet_file = pq.ParquetFile(path.join(check_path,f))

      # Mostrar esquema
      print("📌 Esquema del archivo Parquet:")
      print(parquet_file.schema)

      # Mostrar número de filas
      print(f"\n📊 Número de filas: {parquet_file.metadata.num_rows}")

      # Mostrar número de columnas
      print(f"🗂️  Número de columnas: {parquet_file.metadata.num_columns}")

      # Mostrar número de row groups
      print(f"📦 Número de row groups: {parquet_file.metadata.num_row_groups}")

      # Mostrar metadatos completos
      print("\n📑 Metadatos completos:")
      print(parquet_file.metadata)

    except Exception as e:
      print(f, e)
      file_list.append(f)
  print(f"Los siguientes archivos presentan fallas: {file_list}")
  return file_list
    

def run_wget_command(url, output_file=None):
  """
  Runs a wget command to download a file from the specified URL.
  Optionally saves it with a given output filename.
  
  Parameters:
      url (str): The URL to download.
      output_file (str, optional): The desired name of the output file.
  
  Returns:
      dict: A dictionary containing 'success' (bool), 'stdout' (str), 'stderr' (str)
  """
  # Build command
  command = ["wget", url]
  if output_file:
    command.extend(["-O", output_file])
  
  try:
    # Run command
    result = subprocess.run(
      command,
      check=True,  # Raises CalledProcessError if exit code is non-zero
      capture_output=True,  # Capture stdout and stderr
      text=True  # Return strings instead of bytes
    )
    return {
      "success": True,
      "stdout": result.stdout,
      "stderr": result.stderr
    }
      
  except subprocess.CalledProcessError as e:
    # Handle wget error (non-zero exit code)
    return {
      "success": False,
      "stdout": e.stdout,
      "stderr": e.stderr
    }
  except FileNotFoundError:
    # wget not installed or command not found
    return {
      "success": False,
      "stdout": "",
      "stderr": "wget command not found. Please install wget."
    }
  except Exception as e:
    # Handle unexpected errors
    return {
      "success": False,
      "stdout": "",
      "stderr": str(e)
    }

def run_wget_commands(links: list, output_folder: str):
  print(f"Descargando {len(links)} archivos")

  for (idx, l) in zip(range(1,len(data_links)+1),links):
    filename = l.split("/")[-1]
    output_filepath = path.join( output_folder, filename )
    print(f"Descargando archivo #{idx}: {l}")
    res = run_wget_command(l, output_filepath )

    if not res['success']:
      raise Exception(f"La descarga del archivo {filename} falló: {res['stderr']}")

def validate_parquet_schemas(check_path:str):
  smallest_schema = set()

  new_fields = set()

  check_files = os.listdir(check_path)
  print(f"Revisando {len(check_files)} archivos")

  for f in check_files:

    parquet_file = pq.ParquetFile(path.join(check_path,f))

    if smallest_schema is None:
      smallest_schema = set((field.name, field.physical_type) for field in parquet_file.schema)
    else:
      schema_temp = set((field.name, field.physical_type) for field in parquet_file.schema)

      smallest_schema = smallest_schema & schema_temp
      new_fields = new_fields | (smallest_schema ^ schema_temp)
  
  return smallest_schema, new_fields

if __name__ == "__main__" :

  # URL of the TLC trip data page
  url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

  output_path = "/home/ec2-user/raw/fhvhv_tripdata"
  
  
  data_links = list_parquet_links(url, lambda l: "fhvhv_tripdata" in l )
  # download_files(data_links, output_path)

  run_wget_commands(data_links, output_path)

  check_parquet_files(output_path)

  sch, nf = validate_parquet_schemas(output_path)
  print(f"sch: ")
  pprint.pp(sch)
  print(f"nwf: ")
  pprint.pp(nf)