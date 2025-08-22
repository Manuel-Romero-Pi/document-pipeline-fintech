#!/usr/bin/env python3
"""
Script para eliminar y recrear el índice de Azure Cognitive Search.
Este script elimina el índice actual y crea uno nuevo con el mismo schema.
"""

import os
import sys
import logging
import time
from typing import Optional
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchIndex

# Variable global para el cliente de índice
index_client = None

def setup_logging():    
    """Configura el logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Reducir logs innecesarios de Azure
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
    logging.getLogger('azure.core.pipeline.policies').setLevel(logging.WARNING)
    logging.getLogger('azure.core.pipeline').setLevel(logging.WARNING)
    logging.getLogger('azure.core').setLevel(logging.WARNING)
    logging.getLogger('azure').setLevel(logging.WARNING)

def setup_index_client(endpoint: str, key: str):
    """
    Inicializa el cliente de índice de Azure Cognitive Search.
    
    Args:
        endpoint: Endpoint de Azure Cognitive Search
        key: Clave de API de Azure Cognitive Search
    """
    global index_client
    try:
        credential = AzureKeyCredential(key)
        index_client = SearchIndexClient(endpoint=endpoint, credential=credential)
        logger = logging.getLogger(__name__)
        logger.info("Cliente de índice inicializado exitosamente")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error inicializando cliente de índice: {e}")
        raise

def get_index_schema(index_name: str) -> Optional[SearchIndex]:
    """
    Obtiene el schema del índice actual.
    
    Args:
        index_name: Nombre del índice
        
    Returns:
        SearchIndex object con el schema del índice o None si no existe
    """
    try:
        logger = logging.getLogger(__name__)
        logger.info(f"Obteniendo schema del índice: {index_name}")
        
        global index_client
        if index_client is None:
            raise ValueError("Cliente de índice no inicializado")
        
        index = index_client.get_index(index_name)
        logger.info(f"Schema obtenido exitosamente para el índice: {index_name}")
        return index
    except Exception as e:
        logger.error(f"Error obteniendo schema del índice: {e}")
        return None

def delete_index(index_name: str) -> bool:
    """
    Elimina el índice completo de Azure Cognitive Search.
    
    Args:
        index_name: Nombre del índice
        
    Returns:
        True si la eliminación fue exitosa
    """
    try:
        logger = logging.getLogger(__name__)
        logger.info(f"Eliminando índice: {index_name}")
        
        global index_client
        if index_client is None:
            raise ValueError("Cliente de índice no inicializado")
        
        # Verificar si el índice existe antes de intentar eliminarlo
        try:
            index_client.get_index(index_name)
        except Exception:
            logger.info(f"El índice {index_name} no existe")
            return True
        
        # Eliminar el índice
        index_client.delete_index(index_name)
        
        # Esperar a que la eliminación se complete
        logger.info("Esperando a que la eliminación del índice se complete...")
        time.sleep(5)  # Esperar 5 segundos para que Azure procese la eliminación
        
        logger.info(f"Índice {index_name} eliminado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error eliminando índice: {e}")
        return False

def create_index(index_schema: SearchIndex) -> bool:
    """
    Crea un nuevo índice con el schema proporcionado.
    
    Args:
        index_schema: SearchIndex object con el schema del índice
        
    Returns:
        True si la creación fue exitosa
    """
    try:
        logger = logging.getLogger(__name__)
        index_name = index_schema.name
        logger.info(f"Creando nuevo índice: {index_name}")
        
        global index_client
        if index_client is None:
            raise ValueError("Cliente de índice no inicializado")
        
        # Crear el índice
        index_client.create_index(index_schema)
        
        # Esperar a que la creación se complete
        logger.info("Esperando a que la creación del índice se complete...")
        time.sleep(10)  # Esperar 10 segundos para que Azure procese la creación
        
        # Verificar que el índice se creó correctamente
        try:
            created_index = index_client.get_index(index_name)
            logger.info(f"Índice {index_name} creado exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error verificando la creación del índice: {e}")
            return False
        
    except Exception as e:
        logger.error(f"Error creando índice: {e}")
        return False

def recreate_azure_index():
    """
    Elimina el índice de Azure Cognitive Search y lo recrea con el mismo schema.
    
    Returns:
        bool: True si la recreación fue exitosa, False en caso contrario
    """
    try:
        # Configurar logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Cargar variables de entorno
        load_dotenv()
        
        logger.info("Iniciando recreación del índice de Azure Cognitive Search...")
        
        # Verificar variables de entorno requeridas
        required_vars = [
            "AZURE_SEARCH_ENDPOINT",
            "AZURE_SEARCH_KEY", 
            "AZURE_SEARCH_INDEX_NAME"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Faltan variables de entorno requeridas: {', '.join(missing_vars)}")
            return False
        
        # Obtener variables de entorno
        endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
        key = os.getenv("AZURE_SEARCH_KEY")
        index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")
        
        # Mostrar configuración
        logger.info(f"Endpoint: {endpoint}")
        logger.info(f"Índice: {index_name}")
        
        # Inicializar el cliente de índice
        setup_index_client(endpoint, key)
        
        # Paso 1: Obtener el schema del índice actual
        index_schema = get_index_schema(index_name)
        if not index_schema:
            logger.error("No se pudo obtener el schema del índice actual")
            return False
        
        logger.info("Schema del índice obtenido exitosamente")
        
        # Paso 2: Eliminar el índice actual
        if not delete_index(index_name):
            logger.error("No se pudo eliminar el índice actual")
            return False
        
        logger.info("Índice eliminado exitosamente")
        
        # Paso 3: Crear el nuevo índice con el mismo schema
        if not create_index(index_schema):
            logger.error("No se pudo crear el nuevo índice")
            return False
        
        logger.info("Nuevo índice creado exitosamente")
        logger.info("Recreación del índice completada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error durante la recreación del índice: {e}")
        return False

def main():
    """Función principal del script."""
    
    # Ejecutar la recreación del índice
    success = recreate_azure_index()
    
    # Salir con código apropiado
    if success:
        print("\nRecreación del índice completada exitosamente")
        sys.exit(0)
    else:
        print("\nError durante la recreación del índice")
        sys.exit(1)

if __name__ == "__main__":
    main() 