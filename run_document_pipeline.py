#!/usr/bin/env python3
"""
Document Pipeline Script.
Este script ejecuta el pipeline completo de procesamiento de documentos usando layout-markdown.
"""

import os
import sys
import logging
from dotenv import load_dotenv

def setup_logging(verbose=False):
    """Configura logging basado en el nivel de verbosidad."""
    if verbose:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(message)s'
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s'
        )
        
        # Reducir logs innecesarios de Azure
        logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
        logging.getLogger('azure.core.pipeline.policies').setLevel(logging.WARNING)
        logging.getLogger('azure.core.pipeline').setLevel(logging.WARNING)
        logging.getLogger('azure.core').setLevel(logging.WARNING)
        logging.getLogger('azure').setLevel(logging.WARNING)
        
        logging.getLogger('httpx').setLevel(logging.WARNING)
        logging.getLogger('openai').setLevel(logging.WARNING)
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('pikepdf').setLevel(logging.WARNING)

setup_logging(verbose=False)

sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))

from services.document_pipeline_service import DocumentPipelineService

logger = logging.getLogger(__name__)

def main():
    """Función principal."""
    # Cargar variables de entorno
    load_dotenv()
    
    try:
        # Inicializar pipeline de documentos
        logger.info("Inicializando pipeline de documentos con método layout-markdown")
        pipeline = DocumentPipelineService()
        
        # Ejecutar pipeline completo
        logger.info("Ejecutando pipeline completo de documentos...")
        success = pipeline.run_full_pipeline()
        
        if success:
            logger.info("Pipeline de documentos completado exitosamente")
        else:
            logger.error("Error en el pipeline de documentos")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error ejecutando pipeline: {e}")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
