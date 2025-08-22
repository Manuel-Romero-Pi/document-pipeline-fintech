# Document Pipeline

Un pipeline completo para procesamiento de documentos financieros usando Azure Document Intelligence y Azure Cognitive Search.

## Descripción

Este proyecto implementa un pipeline automatizado para extraer, procesar e indexar documentos financieros (como reportes anuales) utilizando tecnologías de Azure. El pipeline incluye:

- **Extracción de documentos**: Usando Azure Document Intelligence con layout-markdown
- **Chunking inteligente**: División de documentos preservando tablas y estructura
- **Generación de embeddings**: Usando OpenAI para crear representaciones vectoriales
- **Indexación**: Almacenamiento en Azure Cognitive Search para búsquedas semánticas

## Instalación

1. **Clonar el repositorio**:
   ```bash
   git clone <repository-url>
   cd document-pipeline
   ```

2. **Crear entorno virtual**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instalar dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno**:
   Crear un archivo `.env` con las siguientes variables:
   ```
   # Azure Blob Storage
   AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name
   AZURE_STORAGE_ACCOUNT_KEY=your_storage_account_key
   BLOB_CONTAINER_NAME=your_container_name
   
   # Azure Document Intelligence
   AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT=your_document_intelligence_endpoint
   AZURE_DOCUMENT_INTELLIGENCE_KEY=your_document_intelligence_key
   
   # Azure Cognitive Search
   AZURE_SEARCH_ENDPOINT=your_search_endpoint
   AZURE_SEARCH_KEY=your_search_key
   AZURE_SEARCH_INDEX_NAME=your_index_name
   
   # Azure OpenAI
   OPENAI_API_KEY=your_openai_api_key
   OPENAI_API_BASE=your_openai_api_base
   OPENAI_API_VERSION=2024-02-15-preview
   EMBEDDING_DEPLOYMENT_NAME=text-embedding-3-small
   ```

## Uso

### Ejecutar el Pipeline Completo

```bash
python run_document_pipeline.py
```

### Limpiar el Índice de Búsqueda

```bash
python clear_index.py
```

## Características

- **Extracción con layout-markdown**: Preserva la estructura visual de los documentos
- **Chunking inteligente**: Mantiene tablas y relaciones contextuales
- **Procesamiento por lotes**: Optimizado para documentos grandes
- **Logging detallado**: Seguimiento completo del proceso
- **Manejo de errores**: Recuperación robusta ante fallos

## Notas

- Se requiere acceso a servicios de Azure (Document Intelligence y Cognitive Search)
- Se necesita una clave de API de OpenAI para embeddings
