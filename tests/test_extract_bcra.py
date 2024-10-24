import pytest
import pandas as pd
from unittest.mock import patch
from tasks.bcra.extract_bcra import extract_data
import os

# Ejemplo de datos simulados que podría devolver la API
mock_api_response = {
    "results": [
        {'idVariable': 4, 
         'fecha': '2024-09-18', 
         'valor': 994.74
        }
    ]
}

# Función de prueba para el caso exitoso
@patch('requests.get')
def test_extract_data_success(mock_get, tmpdir):
    # Simular la respuesta de la API
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_api_response

    # Contexto de Airflow simulado
    context = {
        'ds': '2024-10-05'
    }

    # Llamar a la función
    parquet_path = extract_data("4", **context)

    # Verificar que el archivo Parquet fue generado
    assert os.path.exists(parquet_path), "El archivo Parquet no fue creado."

    # Cargar el DataFrame del archivo Parquet
    actual_df = pd.read_parquet(parquet_path)

    # DataFrame esperado
    expected_df = pd.DataFrame(mock_api_response['results'])

    # Comparar ambos DataFrames
    pd.testing.assert_frame_equal(actual_df, expected_df)

# Función de prueba para el caso de fallo
@patch('requests.get')
def test_extract_data_failure(mock_get):
    # Simular un error de API
    mock_get.return_value.status_code = 404
    mock_get.return_value.text = "Not Found"

    context = {
        'ds': '2024-10-05'
    }

    # Verificar que se lance una excepción
    with pytest.raises(Exception) as excinfo:
        extract_data("4", **context)

    assert "Failed to fetch data. Status code: 404" in str(excinfo.value)
