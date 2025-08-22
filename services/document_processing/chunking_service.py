import logging
import os
from typing import List
from langchain.schema import Document
from langchain.text_splitter import MarkdownHeaderTextSplitter
import re

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChunkingService:
    """Servicio para realizar chunking semántico de documentos preservando tablas enteras y párrafos completos."""
    
    def __init__(self):
        self._setup_text_splitters()
    

    
    def _setup_text_splitters(self):
        # Configurar MarkdownHeaderTextSplitter para headers principales
        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=[
                ("#", "Header 1"),
                ("##", "Header 2"),
                ("###", "Header 3"),
                ("####", "Header 4"),
            ]
        )
        
        logger.info("MarkdownHeaderTextSplitter configurado")
    
    def _is_table(self, block: str) -> bool:
        """Detecta si un bloque es tabla en markdown, tabulado o LaTeX."""
        if re.search(r"\|.*\|", block):  # markdown
            return True
        if "\t" in block:  # tabulado
            return True
        if "\\begin{tabular}" in block:  # LaTeX
            return True
        return False

    def _split_text_with_tables(self, text: str) -> List[str]:
        """Divide el texto en párrafos y tablas completas sin cortarlos."""
        chunks = []
        blocks = text.split("\n\n")

        for block in blocks:
            block = block.strip()
            if not block:
                continue

            if self._is_table(block):
                chunks.append(block)  # tabla completa como chunk
            else:
                chunks.append(block)  # párrafo completo como chunk

        return chunks

    def _clean_text(self, text: str) -> str:
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        # Preservar saltos de línea, solo limpiar espacios múltiples en la misma línea
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()
    
    def _clean_table_formatting(self, text: str) -> str:
        """Limpia el formato de las tablas HTML eliminando saltos de línea innecesarios."""
        # Buscar bloques de tabla y limpiarlos
        def clean_table_block(match):
            table_content = match.group(0)
            # Eliminar saltos de línea dentro de las etiquetas de tabla
            table_content = re.sub(r'>\s*\n\s*<', '><', table_content)
            # Mantener solo un salto de línea después de </tr>
            table_content = re.sub(r'</tr>\s*\n\s*', '</tr>\n', table_content)
            # Limpiar espacios extra alrededor de las etiquetas
            table_content = re.sub(r'>\s+', '>', table_content)
            table_content = re.sub(r'\s+<', '<', table_content)
            return table_content
        
        # Aplicar limpieza a todas las tablas
        text = re.sub(r'<table>.*?</table>', clean_table_block, text, flags=re.DOTALL)
        return text
    
    def _merge_small_chunks(self, sections: List[str], min_chunk_size: int = 100) -> List[str]:
        """Combina chunks muy pequeños con el siguiente chunk para evitar chunks solo de headers."""
        if not sections:
            return sections
        
        merged_sections = []
        i = 0
        
        while i < len(sections):
            current_section = sections[i]
            
            # Si el chunk actual es muy pequeño y hay un siguiente chunk
            if len(current_section.strip()) < min_chunk_size and i + 1 < len(sections):
                # Combinar con el siguiente chunk
                next_section = sections[i + 1]
                combined_section = current_section + "\n\n" + next_section
                merged_sections.append(combined_section)
                logger.info(f"Combinando chunk pequeño ({len(current_section)} chars) con el siguiente ({len(next_section)} chars)")
                i += 2  # Saltar el siguiente chunk ya que fue combinado
            else:
                merged_sections.append(current_section)
                i += 1
        
        return merged_sections
    
    def _split_by_headers_manual(self, text: str) -> List[str]:
        """División manual por headers usando regex para casos donde MarkdownHeaderTextSplitter falla."""
        # Patrón para detectar headers de markdown (# ## ### ####)
        header_pattern = r'^(#{1,4})\s+(.+)$'
        
        lines = text.split('\n')
        sections = []
        current_section = []
        header_count = 0
        
        logger.info(f"Analizando {len(lines)} líneas para división manual")
        
        for i, line in enumerate(lines):
            line_stripped = line.strip()
            
            # Verificar si es un header
            if re.match(header_pattern, line_stripped):
                header_count += 1
                logger.info(f"Header encontrado en línea {i}: {line_stripped[:50]}...")
                
                # Si encontramos un header y ya tenemos contenido, guardamos la sección anterior
                if current_section:
                    section_text = '\n'.join(current_section).strip()
                    if section_text:  # Solo agregar si no está vacío
                        sections.append(section_text)
                        logger.info(f"Sección guardada con {len(section_text)} caracteres")
                    current_section = []
                
                # Incluir el header en la nueva sección
                current_section.append(line)
            else:
                current_section.append(line)
        
        # Agregar la última sección
        if current_section:
            section_text = '\n'.join(current_section).strip()
            if section_text:  # Solo agregar si no está vacío
                sections.append(section_text)
                logger.info(f"Última sección guardada con {len(section_text)} caracteres")
        
        logger.info(f"División manual completada: {header_count} headers encontrados, {len(sections)} secciones creadas")
        
        # Debug: mostrar las primeras líneas de cada sección
        for i, section in enumerate(sections):
            first_line = section.split('\n')[0].strip()
            logger.info(f"Sección {i}: {first_line[:100]}...")
        
        return sections
    

    
    def chunk_document(self, document: Document) -> List[Document]:
        try:
            cleaned_text = self._clean_text(document.page_content)
            logger.info(f"Texto limpio: {len(cleaned_text)} caracteres")

            # Usar división manual directamente ya que MarkdownHeaderTextSplitter no funciona bien con este contenido
            logger.info("Usando división manual por headers")
            section_texts = self._split_by_headers_manual(cleaned_text)
            
            # Combinar chunks pequeños para evitar headers solos
            section_texts = self._merge_small_chunks(section_texts)
            logger.info(f"Después de combinar chunks pequeños: {len(section_texts)} secciones")
            
            sections = []
            for i, text in enumerate(section_texts):
                if text.strip():
                    # Limpiar formato de tablas
                    cleaned_text_section = self._clean_table_formatting(text.strip())
                    sections.append(Document(
                        page_content=cleaned_text_section,
                        metadata={'section_index': i}
                    ))
            logger.info(f"División manual generó {len(sections)} secciones finales")

            # Procesar cada sección
            chunk_documents = []
            chunk_index = 0

            for section in sections:
                section_content = section.page_content if hasattr(section, 'page_content') else str(section)
                
                # Usar la sección completa como chunk
                chunk_doc = Document(
                    page_content=section_content,
                    metadata={
                        **document.metadata,
                        'chunk_index': chunk_index,
                        'chunk_type': 'header_section',
                        'original_length': len(document.page_content),
                        'section_index': section.metadata.get('section_index', 0) if hasattr(section, 'metadata') else 0
                    }
                )
                chunk_documents.append(chunk_doc)
                chunk_index += 1


            
            logger.info(f"Documento procesado: {len(chunk_documents)} chunks")
            return chunk_documents
            
        except Exception as e:
            logger.error(f"Error en chunking de documento: {e}")
            raise
    
    def chunk_all_documents(self, documents: List[Document]) -> List[Document]:
        try:
            all_chunks = []
            for i, document in enumerate(documents):
                logger.info(f"Procesando documento {i+1}/{len(documents)}: {document.metadata.get('source', 'unknown')}")
                chunks = self.chunk_document(document)
                all_chunks.extend(chunks)
            return all_chunks
        except Exception as e:
            logger.error(f"Error en chunking masivo: {e}")
            raise
    

