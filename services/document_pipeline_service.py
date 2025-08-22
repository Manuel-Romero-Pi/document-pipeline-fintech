import logging
from typing import List
from dotenv import load_dotenv

from .document_processing.document_extraction_service import DocumentExtractionService
from .document_processing.chunking_service import ChunkingService
from .document_processing.embedding_service import EmbeddingService
from .document_processing.indexing_service import IndexingService
from langchain.schema import Document

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentPipelineService:
    """Servicio principal que orquesta el pipeline de procesamiento de documentos."""
    
    def __init__(self, 
                 embedding_batch_size: int = 100,
                 indexing_batch_size: int = 100):
        """
        Inicializa el pipeline de documentos.
        
        Args:
            embedding_batch_size: Tamaño de lote para embeddings
            indexing_batch_size: Tamaño de lote para indexación
        """
        load_dotenv()
        self.embedding_batch_size = embedding_batch_size
        self.indexing_batch_size = indexing_batch_size
        
        # Inicializar servicios
        self.setup_services()
    
    def setup_services(self):
        """Configura todos los servicios del pipeline."""
        try:
            logger.info("Inicializando servicios del pipeline de documentos...")
            
            # Extracción de documentos (solo layout-markdown)
            self.extraction_service = DocumentExtractionService()
            # Chunking de documentos
            self.chunking_service = ChunkingService()
            # Generación de embeddings  
            self.embedding_service = EmbeddingService()
            # Indexación en Azure Cognitive Search
            self.indexing_service = IndexingService()
            
            logger.info("Todos los servicios inicializados exitosamente")
            
        except Exception as e:
            logger.error(f"Error inicializando servicios: {e}")
            raise
    
    def run_full_pipeline(self) -> bool:
        """
        Ejecuta el pipeline completo de documentos.
        
        Returns:
            True si el pipeline se ejecutó exitosamente
        """
        try:
            logger.info("Iniciando pipeline completo de documentos")
            
            # Extracción con layout-markdown
            logger.info("Etapa 1: Extracción de documentos con layout-markdown")
            documents = self.extraction_service.extract_all_documents()
            logger.info(f"Extracción completada: {len(documents)} documentos")
            
            # Chunking con preservación de tablas
            logger.info("Etapa 2: Chunking de documentos con preservación de tablas")
            chunks = self.chunking_service.chunk_all_documents(documents)
            logger.info(f"Chunking completado: {len(chunks)} chunks")
            
            # Embeddings
            logger.info("Etapa 3: Generación de embeddings")
            documents_with_embeddings = self.embedding_service.add_all_embeddings_to_documents(
                chunks, 
                self.embedding_batch_size
            )
            logger.info(f"Embeddings completados: {len(documents_with_embeddings)} documentos con embeddings")
            
            # Indexación
            logger.info("Etapa 4: Indexación en Azure Cognitive Search")
            success = self.indexing_service.index_all_documents(
                documents_with_embeddings, 
                self.indexing_batch_size
            )
            
            if success:
                logger.info("Pipeline de documentos completado exitosamente")
            else:
                logger.error("Error en la indexación")
            
            return success
            
        except Exception as e:
            logger.error(f"Error en el pipeline de documentos: {e}")
            raise
    

