import os
import logging
import tempfile
from typing import List, Dict
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, DocumentContentFormat
from azure.core.credentials import AzureKeyCredential

from langchain.schema import Document

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentExtractionService:
    """Servicio para extraer documentos desde Azure Blob Storage usando layout-markdown."""
    
    def __init__(self):
        load_dotenv()
        self._setup_azure_clients()
    
    def _setup_azure_clients(self):
        """Configura los clientes de Azure necesarios."""
        try:
            # Configuración de Azure Blob Storage
            self.storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
            self.storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
            self.container_name = os.getenv("BLOB_CONTAINER_NAME")
            
            if not all([self.storage_account_name, self.storage_account_key, self.container_name]):
                raise ValueError("Faltan variables de entorno para Azure Blob Storage")
            
            # Cliente de Blob Storage
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Cliente de Document Intelligence
            endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
            key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")
            
            if endpoint and key:
                self.document_intelligence_client = DocumentIntelligenceClient(
                    endpoint=endpoint, 
                    credential=AzureKeyCredential(key)
                )
            else:
                raise ValueError("Document Intelligence no está configurado")
                
            logger.info("Clientes de Azure configurados exitosamente")
            
        except Exception as e:
            logger.error(f"Error configurando clientes de Azure: {e}")
            raise
    
    def list_pending_documents(self) -> List[Dict]:
        """Lista todos los documentos PDF con metadata state='pending' en el contenedor."""
        try:
            documents = []
            blob_list = self.container_client.list_blobs()
            
            for blob in blob_list:
                if blob.name.lower().endswith('.pdf'):
                    # Verificar metadata del blob
                    blob_client = self.container_client.get_blob_client(blob.name)
                    properties = blob_client.get_blob_properties()
                    
                    # Obtener metadata
                    metadata = properties.metadata or {}
                    state = metadata.get('state', 'pending')  # Default a pending si no existe
                    
                    if state == 'pending':
                        documents.append({
                            'name': blob.name,
                            'size': blob.size,
                            'last_modified': blob.last_modified,
                            'metadata': metadata
                        })
            
            logger.info(f"Encontrados {len(documents)} documentos PDF con state='pending'")
            return documents
            
        except Exception as e:
            logger.error(f"Error listando documentos pendientes: {e}")
            raise
    
    def download_blob_to_temp(self, blob_name: str) -> str:
        """Descarga un blob a un archivo temporal."""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                download_stream = blob_client.download_blob()
                temp_file.write(download_stream.readall())
                temp_file_path = temp_file.name
            
            logger.info(f"Descargando: {blob_name}")
            return temp_file_path
            
        except Exception as e:
            logger.error(f"Error descargando blob {blob_name}: {e}")
            raise
    
    def update_blob_state_to_processed(self, blob_name: str) -> bool:
        """Actualiza la metadata del blob cambiando state de 'pending' a 'processed'."""
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            
            # Obtener metadata actual
            properties = blob_client.get_blob_properties()
            current_metadata = properties.metadata or {}
            
            # Actualizar state a processed
            current_metadata['state'] = 'processed'
            
            # Actualizar metadata del blob
            blob_client.set_blob_metadata(metadata=current_metadata)
            
            logger.info(f"Estado actualizado a 'processed': {blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error actualizando estado del blob {blob_name}: {e}")
            return False
    
    def extract_with_document_intelligence(self, file_path: str, blob_name: str) -> Document:
        """Extrae texto usando Azure Document Intelligence con layout-markdown."""
        try:
            if not self.document_intelligence_client:
                raise ValueError("Document Intelligence no está configurado")
            
            logger.info(f"Extrayendo con layout-markdown: {blob_name}")
            
            with open(file_path, "rb") as document:
                document_bytes = document.read()
                poller = self.document_intelligence_client.begin_analyze_document(
                    "prebuilt-layout",
                    AnalyzeDocumentRequest(bytes_source=document_bytes),
                    output_content_format=DocumentContentFormat.MARKDOWN
                )
                result = poller.result()
            
            # Crear documento LangChain
            document = Document(
                page_content=result.content,
                metadata={
                    'source': blob_name,
                    'extraction_method': 'document_intelligence_layout',
                    'pages': len(result.pages) if hasattr(result, 'pages') else 1,
                    'file_path': file_path,
                    'content_format': 'markdown'
                }
            )
            
            logger.info(f"Extracción con layout-markdown completada: {blob_name}")
            return document
            
        except Exception as e:
            logger.error(f"Error en extracción con Document Intelligence: {e}")
            raise
    
    def extract_document(self, blob_name: str) -> Document:
        """Extrae un documento usando Document Intelligence con layout-markdown."""
        try:
            # Descargar documento a archivo temporal
            temp_file_path = self.download_blob_to_temp(blob_name)
            
            try:
                document = self.extract_with_document_intelligence(temp_file_path, blob_name)
                return document
            finally:
                # Limpiar archivo temporal
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    
        except Exception as e:
            logger.error(f"Error extrayendo documento {blob_name}: {e}")
            raise
    
    def extract_all_documents(self) -> List[Document]:
        """Extrae todos los documentos PDF con state='pending' y actualiza su estado a 'processed'."""
        try:
            documents = []
            blob_docs = self.list_pending_documents()
            
            if not blob_docs:
                logger.info("No hay documentos con state='pending' para procesar")
                return documents
            
            for blob_doc in blob_docs:
                try:
                    # Extraer documento
                    document = self.extract_document(blob_doc['name'])
                    documents.append(document)
                    logger.info(f"Documento procesado: {blob_doc['name']}")
                    
                    # Actualizar estado a processed
                    if self.update_blob_state_to_processed(blob_doc['name']):
                        logger.info(f"Estado actualizado a processed: {blob_doc['name']}")
                    else:
                        logger.warning(f"No se pudo actualizar estado: {blob_doc['name']}")
                        
                except Exception as e:
                    logger.error(f"Error procesando documento {blob_doc['name']}: {e}")
                    continue
            
            logger.info(f"Extracción completada: {len(documents)} documentos procesados y estados actualizados")
            return documents
            
        except Exception as e:
            logger.error(f"Error en extracción masiva: {e}")
            raise
