import os
import logging
from typing import List, Optional
from dotenv import load_dotenv
from openai import AzureOpenAI
from langchain.schema import Document

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingService:
    """Servicio para generar embeddings usando OpenAI text-embedding-3-small."""
    
    def __init__(self):
        load_dotenv()
        self._setup_openai_client()
    
    def _setup_openai_client(self):
        """Configura el cliente de OpenAI para embeddings."""
        try:
            # Configuración de Azure OpenAI
            self.api_key = os.getenv("OPENAI_API_KEY")
            self.api_base = os.getenv("OPENAI_API_BASE")
            self.api_version = os.getenv("OPENAI_API_VERSION", "2024-02-15-preview")
            self.deployment_name = os.getenv("EMBEDDING_DEPLOYMENT_NAME", "text-embedding-3-small")
            
            if not all([self.api_key, self.api_base, self.deployment_name]):
                raise ValueError("Faltan variables de entorno para OpenAI embeddings")
            
            # Crear cliente de Azure OpenAI
            self.client = AzureOpenAI(
                azure_endpoint=self.api_base,
                api_key=self.api_key,
                api_version=self.api_version
            )
            
            logger.info(f"Cliente de OpenAI configurado - deployment: {self.deployment_name}")
            
        except Exception as e:
            logger.error(f"Error configurando cliente de OpenAI: {e}")
            raise
    
    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
        """
        Genera embeddings para una lista de textos en lotes.
        
        Args:
            texts: Lista de textos para generar embeddings
            batch_size: Tamaño del lote para procesamiento
            
        Returns:
            Lista de embeddings (puede contener None para textos que fallaron)
        """
        try:
            all_embeddings = []
            total_texts = len(texts)
            
            for i in range(0, total_texts, batch_size):
                batch_texts = texts[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_texts + batch_size - 1) // batch_size
                
                logger.info(f"Procesando lote {batch_num}/{total_batches} ({len(batch_texts)} textos)")
                
                try:
                    # Generar embeddings para el lote
                    response = self.client.embeddings.create(
                        input=batch_texts,
                        model=self.deployment_name
                    )
                    
                    # Extraer embeddings del response
                    batch_embeddings = [data.embedding for data in response.data]
                    all_embeddings.extend(batch_embeddings)
                    
                    logger.info(f"Lote {batch_num} completado: {len(batch_embeddings)} embeddings")
                    
                except Exception as e:
                    logger.error(f"Error en lote {batch_num}: {e}")
                    # Agregar None para cada texto del lote que falló
                    all_embeddings.extend([None] * len(batch_texts))
            
            logger.info(f"Generación de embeddings completada: {len(all_embeddings)} embeddings de {total_texts} textos")
            return all_embeddings
            
        except Exception as e:
            logger.error(f"Error en generación masiva de embeddings: {e}")
            raise
    
    def add_all_embeddings_to_documents(self, documents: List[Document], batch_size: int = 100) -> List[Document]:
        """
        Agrega embeddings a una lista de documentos LangChain.
        
        Args:
            documents: Lista de documentos LangChain
            batch_size: Tamaño del lote para procesamiento
            
        Returns:
            Lista de documentos con embeddings agregados
        """
        try:
            # Extraer textos de los documentos
            texts = [doc.page_content for doc in documents]

            # Generar embeddings
            embeddings = self.generate_embeddings_batch(texts, batch_size)
            
            # Agregar embeddings a los documentos
            documents_with_embeddings = []
            successful_embeddings = 0
            
            for i, (doc, embedding) in enumerate(zip(documents, embeddings)):
                if embedding is not None:
                    # Crear nuevo documento con embedding
                    doc_with_embedding = Document(
                        page_content=doc.page_content,
                        metadata={
                            **doc.metadata,
                            'embedding': embedding,
                            'embedding_dimension': len(embedding),
                        }
                    )
                    documents_with_embeddings.append(doc_with_embedding)
                    successful_embeddings += 1
                else:
                    logger.warning(f"Embedding falló para documento {i}: {doc.metadata.get('source', 'unknown')}")
            
            logger.info(f"Embeddings agregados: {successful_embeddings}/{len(documents)} documentos exitosos")
            return documents_with_embeddings
            
        except Exception as e:
            logger.error(f"Error agregando embeddings a documentos: {e}")
            raise
