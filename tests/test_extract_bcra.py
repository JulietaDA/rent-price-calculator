import pytest
from unittest.mock import patch
from tasks.bcra.extract_bcra import extract_data 

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
def test_extract_data_success():
    with patch('requests.get') as mock_get:
        # Simular la respuesta de la API
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_api_response

        # Contexto de Airflow simulado
        context = {
            'ds': '2024-10-05'
        }

        # Llamar a la función
        df = extract_data("4", **context)

        # Verificar que el DataFrame tenga una sola fila
        assert df.shape[0] == 1
        # Verificar que el DataFrame tenga las columnas esperadas
        expected_columns = ['idVariable', 'fecha', 'valor']
        assert all(col in df.columns for col in expected_columns)

# Función de prueba para el caso de fallo
def test_extract_data_failure():
    with patch('requests.get') as mock_get:
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
