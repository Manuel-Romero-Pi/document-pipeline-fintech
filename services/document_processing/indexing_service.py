import os
import logging
import re
import json
from typing import List
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from langchain.schema import Document

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IndexingService:
    """Servicio para indexar documentos en Azure Cognitive Search."""
    
    def __init__(self):
        load_dotenv()
        self._setup_azure_clients()
    
    def _setup_azure_clients(self):
        """Configura los clientes de Azure Cognitive Search."""
        try:
            # Configuración de Azure Cognitive Search
            self.endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
            self.key = os.getenv("AZURE_SEARCH_KEY")
            self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME")
            
            if not all([self.endpoint, self.key, self.index_name]):
                raise ValueError("Faltan variables de entorno para Azure Cognitive Search")
            
            # Crear credenciales
            self.credential = AzureKeyCredential(self.key)
            
            # Cliente para operaciones de índice
            self.index_client = SearchIndexClient(
                endpoint=self.endpoint,
                credential=self.credential
            )
            
            # Cliente para operaciones de documentos
            self.search_client = SearchClient(
                endpoint=self.endpoint,
                index_name=self.index_name,
                credential=self.credential
            )
            
            logger.info(f"Clientes de Azure Cognitive Search configurados - índice: {self.index_name}")
            
        except Exception as e:
            logger.error(f"Error configurando clientes de Azure Cognitive Search: {e}")
            raise
    
    def _generate_safe_document_key(self, source_name: str, index: int, chunk_index: int) -> str:
        """
        Genera una clave segura para documentos de Azure Cognitive Search.
        
        Args:
            source_name: Nombre del archivo fuente
            index: Índice del documento en la lista
            chunk_index: Índice del chunk
            
        Returns:
            Clave segura que cumple con las restricciones de Azure Cognitive Search
        """
        # Reemplazar caracteres problemáticos con guiones bajos
        safe_source_name = source_name.replace('.', '_').replace('-', '_').replace(' ', '_')
        # Eliminar cualquier otro carácter no permitido (solo letras, dígitos, guiones bajos, guiones y signos igual)
        safe_source_name = re.sub(r'[^a-zA-Z0-9_\-=]', '_', safe_source_name)
        
        return f"{safe_source_name}_{index}_{chunk_index}"
    
    def index_all_documents(self, documents: List[Document], batch_size: int = 100) -> bool:
        """
        Indexa documentos en Azure Cognitive Search.
        
        Args:
            documents: Lista de documentos LangChain con embeddings
            batch_size: Tamaño del lote para indexación
            
        Returns:
            True si la indexación fue exitosa
        """
        try:
            logger.info(f"Iniciando indexación de {len(documents)} documentos")
            
            # Preparar documentos para indexación
            search_documents = []
            documents_with_embeddings = 0
            
            for i, doc in enumerate(documents):
                try:
                    # Verificar que el documento tiene embedding
                    if 'embedding' not in doc.metadata:
                        logger.warning(f"Documento {i} no tiene embedding, saltando")
                        continue
                    
                    documents_with_embeddings += 1
                    
                    # Crear documento para Azure Search
                    source_name = doc.metadata.get('source', 'unknown')
                    chunk_index = doc.metadata.get('chunk_index', 0)
                    
                    search_doc = {
                        "chunk_id": self._generate_safe_document_key(source_name, i, chunk_index),
                        "parent_id": doc.metadata.get('source', 'unknown'),
                        "chunk": doc.page_content,
                        "title": doc.metadata.get('source', 'unknown'),
                        "source": doc.metadata.get('source', 'unknown'),
                        "chunk_type": doc.metadata.get('chunk_type', 'unknown'),
                        "chunk_index": doc.metadata.get('chunk_index', 0),
                        "metadata": json.dumps(doc.metadata, ensure_ascii=False),
                        "text_vector": doc.metadata['embedding']
                    }
                    
                    search_documents.append(search_doc)

                except Exception as e:
                    logger.error(f"Error preparando documento {i}: {e}")
                    continue
            
            if not search_documents:
                logger.error("No hay documentos para indexar")
                return False
            
            # Indexar documentos en lotes
            successful_indexes = 0
            for i in range(0, len(search_documents), batch_size):
                batch_docs = search_documents[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(search_documents) + batch_size - 1) // batch_size
                
                try:
                    # Indexar lote usando merge_or_upload_documents
                    result = self.search_client.merge_or_upload_documents(batch_docs)
                    
                    # Verificar resultados
                    successful_in_batch = sum(1 for r in result if r.succeeded)
                    failed_in_batch = sum(1 for r in result if not r.succeeded)
                    successful_indexes += successful_in_batch
                    
                    logger.info(f"Lote {batch_num}/{total_batches} indexado: {successful_in_batch}/{len(batch_docs)} exitosos, {failed_in_batch} fallidos")
                    
                    # Log errores si los hay
                    if failed_in_batch > 0:
                        for j, r in enumerate(result):
                            if not r.succeeded:
                                logger.error(f"Error en documento {j} del lote {batch_num}: {r.status_code} - {r.message}")
                    
                except Exception as e:
                    logger.error(f"Error indexando lote {batch_num}: {e}")
                    continue
            
            logger.info(f"Indexación completada: {successful_indexes}/{len(search_documents)} documentos indexados exitosamente")
            return successful_indexes > 0
            
        except Exception as e:
            logger.error(f"Error en indexación masiva: {e}")
            raise
