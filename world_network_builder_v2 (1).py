#!/usr/bin/env python3
"""
Phase 1: World Network Builder - Complete Single Script
========================================================
Transforms SOP/DLP documents into structured, versioned, deterministic executable graphs
with interactive tree visualization.

Usage:
    python world_network_complete.py <pdf_path> <output_dir>

Example:
    python world_network_complete.py ./P966.pdf ./output
"""

import re
import json
import hashlib
import sys
import os
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from collections import defaultdict
import uuid

# ============================================================================
# ENUMS AND CONSTANTS
# ============================================================================

class NodeType(Enum):
    """Types of nodes in the World Network"""
    ROOT = "root"
    CLAIM_TYPE = "claim_type"
    DECISION = "decision"
    CONDITION = "condition"
    ACTION = "action"
    BRANCH_YES = "branch_yes"
    BRANCH_NO = "branch_no"
    BRANCH_UNSURE = "branch_unsure"
    SUB_DECISION = "sub_decision"
    REFERENCE = "reference"
    TABLE = "table"
    NOTE = "note"
    TERMINAL = "terminal"
    STEP = "step"
    LOOKUP_TABLE = "lookup_table"


class EdgeType(Enum):
    """Types of edges in the World Network"""
    SEQUENCE = "sequence"
    CONDITION_YES = "condition_yes"
    CONDITION_NO = "condition_no"
    CONDITION_UNSURE = "condition_unsure"
    NESTED_YES = "nested_yes"
    NESTED_NO = "nested_no"
    REFERENCE = "reference"
    CONTAINS = "contains"
    CONTINUE_TO_STEP = "continue_to_step"
    PROCEED_TO_SECTION = "proceed_to_section"
    LOOKUP = "lookup"


class EntityType(Enum):
    """Types of entities in Observation Network"""
    PROVIDER_ID = "provider_id"
    PROVIDER_NAME = "provider_name"
    TIN = "tin"
    NPI = "npi"
    PEND_CODE = "pend_code"
    PCA_CODE = "pca_code"
    GROUP_NUMBER = "group_number"
    PROCEDURE_CODE = "procedure_code"
    ULTRA_BLUE_MESSAGE = "ultra_blue_message"
    EXPLANATION_CODE = "explanation_code"
    CLINIC = "clinic"


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class Entity:
    """Represents an extracted entity for Observation Network"""
    id: str
    name: str
    entity_type: str
    value: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    mentions: List[Dict] = field(default_factory=list)
    relationships: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class NetworkNode:
    """Represents a node in the World Network"""
    id: str
    node_type: NodeType
    content: str
    section: Optional[str] = None
    step_number: Optional[int] = None
    parent_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    entities: List[str] = field(default_factory=list)
    source_text: str = ""
    source_line: int = 0
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['node_type'] = self.node_type.value
        return d


@dataclass
class NetworkEdge:
    """Represents an edge in the World Network"""
    id: str
    source_id: str
    target_id: str
    edge_type: EdgeType
    condition: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['edge_type'] = self.edge_type.value
        return d


@dataclass
class ProcedureReference:
    """Reference to another procedure (deep link)"""
    id: str
    procedure_code: str
    title: str = ""
    status: str = "pending"
    resolved_network: Optional[Any] = None
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['resolved_network'] = None
        return d


@dataclass
class VersionInfo:
    """Version information for the document"""
    revision: str
    date: str
    author: str = ""
    description: str = ""
    changes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class LookupTable:
    """Represents a lookup table (provider IDs, clinics, etc.)"""
    id: str
    name: str
    table_type: str
    entries: List[Dict[str, str]] = field(default_factory=list)
    headers: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return asdict(self)


# ============================================================================
# WORLD NETWORK CLASS
# ============================================================================

class WorldNetwork:
    """Main World Network data structure"""
    
    def __init__(self, document_id: str, document_name: str):
        self.document_id = document_id
        self.document_name = document_name
        self.current_version = "1.0"
        self.nodes: Dict[str, NetworkNode] = {}
        self.edges: Dict[str, NetworkEdge] = {}
        self.entities: Dict[str, Entity] = {}
        self.procedure_refs: Dict[str, ProcedureReference] = {}
        self.versions: List[VersionInfo] = []
        self.claim_type_roots: Dict[str, str] = {}
        self.lookup_tables: Dict[str, LookupTable] = {}
        self.metadata: Dict[str, Any] = {}
        self._node_counter = 0
        self._edge_counter = 0
        
    def create_node(self, node_type: NodeType, content: str, **kwargs) -> NetworkNode:
        self._node_counter += 1
        node_id = f"node_{self._node_counter:04d}"
        node = NetworkNode(id=node_id, node_type=node_type, content=content, **kwargs)
        self.nodes[node_id] = node
        return node
        
    def create_edge(self, source_id: str, target_id: str, edge_type: EdgeType, 
                    condition: Optional[str] = None) -> NetworkEdge:
        self._edge_counter += 1
        edge_id = f"edge_{self._edge_counter:04d}"
        edge = NetworkEdge(id=edge_id, source_id=source_id, target_id=target_id,
                          edge_type=edge_type, condition=condition)
        self.edges[edge_id] = edge
        return edge
    
    def get_outgoing_edges(self, node_id: str) -> List[NetworkEdge]:
        return [e for e in self.edges.values() if e.source_id == node_id]
    
    def get_incoming_edges(self, node_id: str) -> List[NetworkEdge]:
        return [e for e in self.edges.values() if e.target_id == node_id]
    
    def to_dict(self) -> Dict:
        return {
            'document_id': self.document_id,
            'document_name': self.document_name,
            'current_version': self.current_version,
            'nodes': {k: v.to_dict() for k, v in self.nodes.items()},
            'edges': {k: v.to_dict() for k, v in self.edges.items()},
            'entities': {k: v.to_dict() for k, v in self.entities.items()},
            'procedure_refs': {k: v.to_dict() for k, v in self.procedure_refs.items()},
            'versions': [v.to_dict() for v in self.versions],
            'claim_type_roots': self.claim_type_roots,
            'lookup_tables': {k: v.to_dict() for k, v in self.lookup_tables.items()},
            'metadata': self.metadata
        }


# ============================================================================
# SOP PARSER
# ============================================================================

class SOPParser:
    """Parse SOP document text into structured sections"""
    
    CLAIM_TYPE_PATTERNS = [
        r'###\s*\*\*\s*(Amazon\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Alaska\s+Air.*?Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Microsoft\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Expedia\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(FEP\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(LEOFF.*?)\s*\*\*',
        r'###\s*\*\*\s*(All\s+Others?)\s*\*\*',
        r'^##\s*\*\*\s*(Amazon\s+Claims?)\s*\*\*',
        r'^##\s*\*\*\s*(All\s+Others?)\s*\*\*',
        r'^#+\s*(Amazon\s+Claims?)',
        r'^#+\s*(Alaska\s+Air.*?Claims?)',
        r'^#+\s*(Microsoft\s+Claims?)',
        r'^#+\s*(Expedia\s+Claims?)',
        r'^#+\s*(FEP\s+Claims?)',
        r'^#+\s*(LEOFF.*?Claims?)',
        r'^#+\s*(All\s+Others?)',
    ]
    
    STEP_PATTERN = re.compile(r'^(\d+)\.\s*(.+)', re.MULTILINE)
    YES_NO_PATTERN = re.compile(r'\*?\*?(?:I\s+)?(Yes|No|Unsure)\s*[:\*\*]*\s*(.+)', re.IGNORECASE)
    DECISION_PATTERN = re.compile(r'^(?:Is|Does|Did|Are|Has|Have|Was|Were|Can|Should|Will|Would)\s+', re.IGNORECASE)
    PROVIDER_ID_PATTERN = re.compile(r'\b([A-Z]\d{2}[A-Z0-9]{3}[A-Z]\d{2}[A-Z0-9]{3}|\b[A-Z]\d{2}[A-Z0-9]{2,3}[A-Z][A-Z0-9]{2,3})\b')
    TIN_PATTERN = re.compile(r'\b(\d{9})\b|\bTIN\s*[:#]?\s*(\d{9}|\d{2}-\d{7})\b', re.IGNORECASE)
    PROCEDURE_REF_PATTERN = re.compile(r'(PR\.OP\.CL\.\d{4})')
    PCA_PATTERN = re.compile(r'\bPCA\s+([A-Z]?\d{3,4})\b')
    PEND_CODE_PATTERN = re.compile(r'\bpend\s+(?:code\s+)?([A-Z]\d{2,3}|\d{3,4})\b', re.IGNORECASE)
    
    VERSION_PATTERN = re.compile(r'^\|\s*(\d+\.\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{4}[^|]*)\s*\|\s*([^|]+)\s*\|', re.MULTILINE)
    
    def __init__(self):
        self.parsed_data = {}
        
    def parse(self, text: str) -> Dict:
        """Parse SOP text into structured data"""
        self.parsed_data = {
            'document_info': self._extract_document_info(text),
            'versions': self._extract_versions(text),
            'sections': self._extract_sections(text),
            'tables': self._extract_tables(text),
            'raw_text': text
        }
        return self.parsed_data
    
    def _extract_document_info(self, text: str) -> Dict:
        """Extract document metadata"""
        info = {
            'title': '',
            'document_id': '',
            'effective_date': '',
            'status': ''
        }
        
        # Extract title
        title_match = re.search(r'^#\s+(.+?)(?:\n|$)', text, re.MULTILINE)
        if title_match:
            info['title'] = title_match.group(1).strip()
        
        # Extract document ID (P966, etc.)
        doc_id_match = re.search(r'\b(P\d{3,4})\b', text)
        if doc_id_match:
            info['document_id'] = doc_id_match.group(1)
        
        # Extract status
        if 'CURRENT' in text.upper():
            info['status'] = 'Current'
        elif 'DRAFT' in text.upper():
            info['status'] = 'Draft'
            
        return info
    
    def _extract_versions(self, text: str) -> List[Dict]:
        """Extract version history from the document"""
        versions = []
        
        for match in self.VERSION_PATTERN.finditer(text):
            versions.append({
                'revision': match.group(1),
                'date': match.group(2).strip(),
                'description': match.group(3).strip()
            })
        
        return versions
    
    def _extract_sections(self, text: str) -> List[Dict]:
        """Extract claim type sections and their steps"""
        sections = []
        
        # Find all claim type sections
        section_matches = []
        for pattern in self.CLAIM_TYPE_PATTERNS:
            for match in re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE):
                section_matches.append((match.start(), match.group(1)))
        
        # Sort by position
        section_matches.sort(key=lambda x: x[0])
        
        # Extract content for each section
        for i, (start_pos, section_name) in enumerate(section_matches):
            end_pos = section_matches[i + 1][0] if i + 1 < len(section_matches) else len(text)
            section_text = text[start_pos:end_pos]
            
            section_data = {
                'name': section_name.strip(),
                'steps': self._extract_steps(section_text),
                'raw_text': section_text
            }
            sections.append(section_data)
        
        return sections
    
    def _extract_steps(self, section_text: str) -> List[Dict]:
        """Extract numbered steps from a section"""
        steps = []
        
        # Find all numbered steps
        step_matches = list(self.STEP_PATTERN.finditer(section_text))
        
        for i, match in enumerate(step_matches):
            step_num = int(match.group(1))
            step_start = match.start()
            step_end = step_matches[i + 1].start() if i + 1 < len(step_matches) else len(section_text)
            step_text = section_text[step_start:step_end].strip()
            
            # Determine if this is a decision step
            step_content = match.group(2).strip()
            is_decision = bool(self.DECISION_PATTERN.search(step_content)) or '?' in step_content
            
            step_data = {
                'number': step_num,
                'content': step_content,
                'full_text': step_text,
                'is_decision': is_decision,
                'branches': self._extract_branches(step_text) if is_decision else [],
                'entities': self._extract_entities_from_text(step_text)
            }
            steps.append(step_data)
        
        return steps
    
    def _extract_branches(self, step_text: str) -> List[Dict]:
        """Extract Yes/No/Unsure branches from step text"""
        branches = []
        
        # Split by Yes/No patterns
        lines = step_text.split('\n')
        current_branch = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check for Yes/No/Unsure markers
            yes_match = re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Yes)\s*[:\*\*]*\s*(.*)', line, re.IGNORECASE)
            no_match = re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(No)\s*[:\*\*]*\s*(.*)', line, re.IGNORECASE)
            unsure_match = re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Unsure)\s*[:\*\*]*\s*(.*)', line, re.IGNORECASE)
            
            if yes_match:
                if current_branch:
                    branches.append(current_branch)
                current_branch = {
                    'type': 'yes',
                    'content': yes_match.group(2).strip(),
                    'sub_decisions': []
                }
            elif no_match:
                if current_branch:
                    branches.append(current_branch)
                current_branch = {
                    'type': 'no',
                    'content': no_match.group(2).strip(),
                    'sub_decisions': []
                }
            elif unsure_match:
                if current_branch:
                    branches.append(current_branch)
                current_branch = {
                    'type': 'unsure',
                    'content': unsure_match.group(2).strip(),
                    'sub_decisions': []
                }
            elif current_branch:
                # Add to current branch content
                current_branch['content'] += ' ' + line
        
        if current_branch:
            branches.append(current_branch)
        
        return branches
    
    def _extract_entities_from_text(self, text: str) -> Dict[str, List[str]]:
        """Extract entities from text"""
        entities = {
            'provider_ids': list(set(self.PROVIDER_ID_PATTERN.findall(text))),
            'tins': [],
            'procedure_refs': list(set(self.PROCEDURE_REF_PATTERN.findall(text))),
            'pca_codes': list(set(self.PCA_PATTERN.findall(text))),
            'pend_codes': list(set(self.PEND_CODE_PATTERN.findall(text)))
        }
        
        # Extract TINs
        for match in self.TIN_PATTERN.finditer(text):
            tin = match.group(1) or match.group(2)
            if tin:
                entities['tins'].append(tin.replace('-', ''))
        entities['tins'] = list(set(entities['tins']))
        
        return entities
    
    def _extract_tables(self, text: str) -> List[Dict]:
        """Extract tables from markdown text"""
        tables = []
        
        # Find markdown tables
        table_pattern = re.compile(r'(\|[^\n]+\|\n\|[-:\s|]+\|\n(?:\|[^\n]+\|\n?)+)', re.MULTILINE)
        
        for match in table_pattern.finditer(text):
            table_text = match.group(1)
            lines = [l.strip() for l in table_text.strip().split('\n') if l.strip()]
            
            if len(lines) >= 2:
                # Parse headers
                headers = [h.strip() for h in lines[0].split('|') if h.strip()]
                
                # Parse rows (skip separator line)
                rows = []
                for line in lines[2:]:
                    cells = [c.strip() for c in line.split('|') if c.strip()]
                    if cells:
                        row = dict(zip(headers, cells))
                        rows.append(row)
                
                tables.append({
                    'headers': headers,
                    'rows': rows,
                    'raw_text': table_text
                })
        
        return tables


# ============================================================================
# WORLD NETWORK BUILDER
# ============================================================================

class WorldNetworkBuilder:
    """Build World Network from parsed SOP data"""
    
    def __init__(self):
        self.network = None
        self.entity_counter = 0
        
    def build(self, parsed_data: Dict, document_id: str, document_name: str) -> WorldNetwork:
        """Build the complete World Network"""
        self.network = WorldNetwork(document_id, document_name)
        
        # Extract version info
        doc_info = parsed_data.get('document_info', {})
        self.network.metadata = {
            'title': doc_info.get('title', ''),
            'status': doc_info.get('status', ''),
            'effective_date': doc_info.get('effective_date', '')
        }
        
        # Add version history
        for v in parsed_data.get('versions', []):
            self.network.versions.append(VersionInfo(
                revision=v.get('revision', ''),
                date=v.get('date', ''),
                description=v.get('description', '')
            ))
        
        if self.network.versions:
            self.network.current_version = self.network.versions[0].revision
        
        # Create root node
        root_node = self.network.create_node(
            NodeType.ROOT,
            document_name,
            metadata={'document_id': document_id}
        )
        
        # Process each claim type section
        for section in parsed_data.get('sections', []):
            self._process_section(section, root_node.id)
        
        # Extract lookup tables
        for table in parsed_data.get('tables', []):
            self._process_table(table)
        
        # Extract all entities
        self._extract_all_entities(parsed_data)
        
        return self.network
    
    def _process_section(self, section: Dict, parent_id: str):
        """Process a claim type section"""
        section_name = section['name']
        
        # Create claim type node
        claim_type_node = self.network.create_node(
            NodeType.CLAIM_TYPE,
            section_name,
            section=section_name
        )
        
        # Link to parent
        self.network.create_edge(parent_id, claim_type_node.id, EdgeType.CONTAINS)
        
        # Store as claim type root
        self.network.claim_type_roots[section_name] = claim_type_node.id
        
        # Process steps
        prev_node_id = claim_type_node.id
        for step in section.get('steps', []):
            prev_node_id = self._process_step(step, claim_type_node.id, prev_node_id, section_name)
    
    def _process_step(self, step: Dict, section_id: str, prev_node_id: str, section_name: str) -> str:
        """Process a single step"""
        step_num = step['number']
        content = step['content']
        is_decision = step['is_decision']
        
        if is_decision:
            # Create decision node
            decision_node = self.network.create_node(
                NodeType.DECISION,
                content,
                section=section_name,
                step_number=step_num,
                source_text=step.get('full_text', '')
            )
            
            # Link from previous
            self.network.create_edge(prev_node_id, decision_node.id, EdgeType.SEQUENCE)
            
            # Process branches
            last_branch_id = decision_node.id
            for branch in step.get('branches', []):
                branch_id = self._process_branch(branch, decision_node.id, section_name)
                last_branch_id = branch_id
            
            # Extract procedure references
            self._extract_procedure_refs(step.get('full_text', ''), decision_node.id)
            
            return decision_node.id
        else:
            # Create step node
            step_node = self.network.create_node(
                NodeType.STEP,
                content,
                section=section_name,
                step_number=step_num,
                source_text=step.get('full_text', '')
            )
            
            # Link from previous
            self.network.create_edge(prev_node_id, step_node.id, EdgeType.SEQUENCE)
            
            return step_node.id
    
    def _process_branch(self, branch: Dict, parent_id: str, section_name: str) -> str:
        """Process a Yes/No/Unsure branch"""
        branch_type = branch['type'].lower()
        content = branch['content']
        
        # Determine node type and edge type
        if branch_type == 'yes':
            node_type = NodeType.BRANCH_YES
            edge_type = EdgeType.CONDITION_YES
            condition = "YES"
        elif branch_type == 'no':
            node_type = NodeType.BRANCH_NO
            edge_type = EdgeType.CONDITION_NO
            condition = "NO"
        else:
            node_type = NodeType.BRANCH_UNSURE
            edge_type = EdgeType.CONDITION_UNSURE
            condition = "UNSURE"
        
        # Check if content has sub-decisions
        has_sub_decision = '?' in content and bool(re.search(r'\b(Is|Does|Are|Has|Can)\b', content, re.IGNORECASE))
        
        if has_sub_decision:
            # Create branch node
            branch_node = self.network.create_node(
                node_type,
                content.split('?')[0].strip() + '?',
                section=section_name
            )
        else:
            branch_node = self.network.create_node(
                node_type,
                content,
                section=section_name
            )
        
        # Link to parent
        self.network.create_edge(parent_id, branch_node.id, edge_type, condition)
        
        # Extract procedure references
        self._extract_procedure_refs(content, branch_node.id)
        
        # Process nested sub-decisions if present
        if has_sub_decision and '?' in content:
            parts = content.split('?')
            if len(parts) > 1:
                remaining = '?'.join(parts[1:]).strip()
                if remaining:
                    self._process_nested_content(remaining, branch_node.id, section_name)
        
        return branch_node.id
    
    def _process_nested_content(self, content: str, parent_id: str, section_name: str):
        """Process nested Yes/No content within a branch"""
        # Look for Yes:/No: patterns
        yes_match = re.search(r'\*?\*?(?:I\s+)?Yes\s*[:\*\*]+\s*(.+?)(?=\*?\*?(?:I\s+)?No|$)', content, re.IGNORECASE | re.DOTALL)
        no_match = re.search(r'\*?\*?(?:I\s+)?No\s*[:\*\*]+\s*(.+?)(?=$)', content, re.IGNORECASE | re.DOTALL)
        
        if yes_match:
            yes_content = yes_match.group(1).strip()
            yes_node = self.network.create_node(
                NodeType.BRANCH_YES,
                yes_content,
                section=section_name
            )
            self.network.create_edge(parent_id, yes_node.id, EdgeType.NESTED_YES, "YES")
            self._extract_procedure_refs(yes_content, yes_node.id)
        
        if no_match:
            no_content = no_match.group(1).strip()
            no_node = self.network.create_node(
                NodeType.BRANCH_NO,
                no_content,
                section=section_name
            )
            self.network.create_edge(parent_id, no_node.id, EdgeType.NESTED_NO, "NO")
            self._extract_procedure_refs(no_content, no_node.id)
    
    def _extract_procedure_refs(self, text: str, node_id: str):
        """Extract and store procedure references"""
        pattern = re.compile(r'(PR\.OP\.CL\.\d{4})')
        
        for match in pattern.finditer(text):
            proc_code = match.group(1)
            
            if proc_code not in self.network.procedure_refs:
                # Extract title if present
                title_match = re.search(rf'{proc_code}\s*[-–]\s*([^.]+)', text)
                title = title_match.group(1).strip() if title_match else ""
                
                ref = ProcedureReference(
                    id=f"ref_{proc_code}",
                    procedure_code=proc_code,
                    title=title
                )
                self.network.procedure_refs[proc_code] = ref
            
            # Create reference node
            ref_node = self.network.create_node(
                NodeType.REFERENCE,
                f"Refer to: {proc_code}",
                metadata={'procedure_code': proc_code}
            )
            self.network.create_edge(node_id, ref_node.id, EdgeType.REFERENCE)
    
    def _process_table(self, table: Dict):
        """Process a lookup table"""
        headers = table.get('headers', [])
        rows = table.get('rows', [])
        
        if not headers or not rows:
            return
        
        # Determine table type
        table_type = 'general'
        if any('clinic' in h.lower() for h in headers):
            table_type = 'clinics'
        elif any('provider' in h.lower() for h in headers):
            table_type = 'providers'
        
        # Create lookup table
        table_id = f"table_{len(self.network.lookup_tables) + 1:03d}"
        lookup_table = LookupTable(
            id=table_id,
            name=f"{table_type.title()} Lookup",
            table_type=table_type,
            headers=headers,
            entries=rows
        )
        self.network.lookup_tables[table_id] = lookup_table
    
    def _extract_all_entities(self, parsed_data: Dict):
        """Extract all entities from the parsed data"""
        text = parsed_data.get('raw_text', '')
        
        # Provider IDs
        for pid in re.findall(r'\b([A-Z]\d{2}[A-Z0-9]{3}[A-Z]\d{2}[A-Z0-9]{3})\b', text):
            self._add_entity(pid, EntityType.PROVIDER_ID.value, pid)
        
        # Provider names
        provider_names = [
            'Vita Health', 'Concentra', 'Crossover', 'MedAire', 'Omada', 
            'Physera', 'Kabafusion', 'Progyny', '98POINT6', 'UPMC', 'VSP'
        ]
        for name in provider_names:
            if name.lower() in text.lower():
                self._add_entity(name, EntityType.PROVIDER_NAME.value, name)
        
        # TINs
        for match in re.finditer(r'\bTIN\s*[:#]?\s*(\d{9}|\d{2}-\d{7})\b|\b(\d{9})\b', text, re.IGNORECASE):
            tin = (match.group(1) or match.group(2) or '').replace('-', '')
            if tin and len(tin) == 9:
                self._add_entity(tin, EntityType.TIN.value, tin)
        
        # PCA codes
        for pca in re.findall(r'\bPCA\s+([A-Z]?\d{3,4})\b', text):
            self._add_entity(f"PCA_{pca}", EntityType.PCA_CODE.value, pca)
        
        # Pend codes
        for pend in re.findall(r'\bpend\s+(?:code\s+)?([A-Z]\d{2,3})\b', text, re.IGNORECASE):
            self._add_entity(f"PEND_{pend}", EntityType.PEND_CODE.value, pend)
        
        # Ultra Blue messages
        for msg in re.findall(r'\b(BCD|BOK|BVT)\s*[-–]?\s*([A-Z\s]+)', text):
            code = msg[0]
            self._add_entity(f"UB_{code}", EntityType.ULTRA_BLUE_MESSAGE.value, code)
    
    def _add_entity(self, name: str, entity_type: str, value: str):
        """Add an entity to the network"""
        entity_id = f"entity_{hashlib.md5(f'{entity_type}_{value}'.encode()).hexdigest()[:8]}"
        
        if entity_id not in self.network.entities:
            self.network.entities[entity_id] = Entity(
                id=entity_id,
                name=name,
                entity_type=entity_type,
                value=value
            )


# ============================================================================
# TREE VISUALIZATION GENERATOR
# ============================================================================

class TreeVisualizationGenerator:
    """Generate interactive HTML tree visualization"""
    
    @staticmethod
    def generate(network: WorldNetwork) -> str:
        """Generate interactive HTML tree visualization using SVG"""
        
        # Build hierarchical tree data for each claim type
        def build_tree(node_id, visited=None):
            if visited is None:
                visited = set()
            if node_id in visited or node_id not in network.nodes:
                return None
            visited.add(node_id)
            
            node = network.nodes[node_id]
            label = node.content[:40].replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;').replace('\n', ' ')
            if node.step_number:
                label = f"S{node.step_number}: {label}"
            
            children = []
            for edge in network.get_outgoing_edges(node_id):
                child_tree = build_tree(edge.target_id, visited.copy())
                if child_tree:
                    child_tree['edgeLabel'] = (edge.condition or '').replace('"', '&quot;')
                    children.append(child_tree)
            
            return {
                'id': node_id,
                'name': label,
                'type': node.node_type.value,
                'fullContent': node.content[:150].replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;').replace('\n', ' '),
                'children': children
            }
        
        # Build trees for each claim type
        trees_data = {}
        for claim_type, root_id in network.claim_type_roots.items():
            tree = build_tree(root_id)
            if tree:
                trees_data[claim_type] = tree
        
        # Escape for JSON embedding
        trees_json = json.dumps(trees_data).replace('</script>', '<\\/script>')
        
        # Build claim type buttons
        claim_buttons = ""
        first = True
        for ct_name in network.claim_type_roots.keys():
            active = "active" if first else ""
            safe_name = ct_name.replace("'", "\\'").replace('"', '&quot;')
            display_name = ct_name.replace('"', '&quot;').replace('<', '&lt;')
            claim_buttons += f'<button class="claim-btn {active}" onclick="showClaimType(\'{safe_name}\')">{display_name}</button>'
            first = False
        
        html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>World Network Tree - {network.document_name}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 15px 20px;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 100;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .header h1 {{ 
            font-size: 18px; 
            color: white; 
            margin-bottom: 8px;
            font-weight: 600;
        }}
        .header-info {{ 
            font-size: 12px; 
            color: rgba(255,255,255,0.8); 
            margin-bottom: 10px; 
        }}
        .claim-buttons {{ display: flex; gap: 8px; flex-wrap: wrap; }}
        .claim-btn {{
            padding: 6px 14px;
            border: none;
            background: rgba(255,255,255,0.2);
            color: white;
            border-radius: 20px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 500;
            transition: all 0.3s;
        }}
        .claim-btn:hover {{ background: rgba(255,255,255,0.3); }}
        .claim-btn.active {{ 
            background: white; 
            color: #667eea; 
        }}
        
        .legend {{
            position: fixed;
            top: 110px;
            right: 15px;
            background: white;
            padding: 12px 15px;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            z-index: 100;
            font-size: 11px;
        }}
        .legend-title {{ 
            font-weight: 600; 
            margin-bottom: 8px; 
            color: #333;
            font-size: 12px;
        }}
        .legend-item {{ 
            display: flex; 
            align-items: center; 
            gap: 8px; 
            margin: 5px 0;
            color: #555;
        }}
        .legend-diamond {{ 
            width: 12px; 
            height: 12px; 
            background: #2196f3; 
            transform: rotate(45deg); 
            border-radius: 2px; 
        }}
        .legend-rect {{ width: 16px; height: 10px; border-radius: 3px; }}
        
        .svg-container {{
            margin-top: 100px;
            overflow: auto;
            height: calc(100vh - 100px);
            cursor: grab;
            background: linear-gradient(45deg, #f5f7fa 25%, transparent 25%),
                        linear-gradient(-45deg, #f5f7fa 25%, transparent 25%),
                        linear-gradient(45deg, transparent 75%, #f5f7fa 75%),
                        linear-gradient(-45deg, transparent 75%, #f5f7fa 75%);
            background-size: 20px 20px;
            background-position: 0 0, 0 10px, 10px -10px, -10px 0px;
            background-color: #eef1f5;
        }}
        .svg-container:active {{ cursor: grabbing; }}
        
        #treeSvg {{
            display: block;
            min-width: 100%;
            min-height: 100%;
        }}
        
        .tooltip {{
            position: fixed;
            background: rgba(30,30,30,0.95);
            color: white;
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 12px;
            max-width: 320px;
            pointer-events: none;
            z-index: 1000;
            display: none;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            line-height: 1.5;
            border: 1px solid rgba(255,255,255,0.1);
        }}
        .tooltip strong {{ 
            color: #64b5f6; 
            text-transform: capitalize;
        }}
        
        .controls {{
            position: fixed;
            bottom: 20px;
            left: 20px;
            display: flex;
            gap: 8px;
            z-index: 100;
        }}
        .control-btn {{
            width: 40px;
            height: 40px;
            border: none;
            background: white;
            border-radius: 50%;
            cursor: pointer;
            box-shadow: 0 2px 10px rgba(0,0,0,0.15);
            font-size: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.2s;
            color: #555;
        }}
        .control-btn:hover {{ 
            background: #667eea; 
            color: white;
            transform: scale(1.1);
        }}
        
        .stats {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: white;
            padding: 10px 15px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            font-size: 11px;
            color: #666;
            z-index: 100;
        }}
        .stats span {{ 
            font-weight: 600; 
            color: #667eea; 
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>&#x1F333; {network.document_name}</h1>
        <div class="header-info">
            Document: {network.document_id} | Version: {network.current_version} | 
            Nodes: {len(network.nodes)} | Edges: {len(network.edges)}
        </div>
        <div class="claim-buttons">
            {claim_buttons}
        </div>
    </div>
    
    <div class="legend">
        <div class="legend-title">Node Types</div>
        <div class="legend-item"><div class="legend-diamond"></div> Decision Point</div>
        <div class="legend-item"><div class="legend-rect" style="background:#4caf50"></div> Yes Branch</div>
        <div class="legend-item"><div class="legend-rect" style="background:#f44336"></div> No Branch</div>
        <div class="legend-item"><div class="legend-rect" style="background:#ff9800"></div> Unsure</div>
        <div class="legend-item"><div class="legend-rect" style="background:#9c27b0"></div> Claim Type</div>
        <div class="legend-item"><div class="legend-rect" style="background:#607d8b"></div> Action/Step</div>
        <div class="legend-item"><div class="legend-rect" style="background:#e91e63"></div> Reference</div>
    </div>
    
    <div class="controls">
        <button class="control-btn" onclick="zoomIn()" title="Zoom In">+</button>
        <button class="control-btn" onclick="zoomOut()" title="Zoom Out">−</button>
        <button class="control-btn" onclick="resetView()" title="Reset View">↺</button>
    </div>
    
    <div class="stats" id="stats">
        Nodes: <span id="nodeCount">0</span> | 
        Depth: <span id="treeDepth">0</span>
    </div>
    
    <div class="tooltip" id="tooltip"></div>
    
    <div class="svg-container" id="svgContainer">
        <svg id="treeSvg"></svg>
    </div>
    
    <script>
        const treesData = {trees_json};
        const claimTypes = Object.keys(treesData);
        let currentClaimType = claimTypes[0] || '';
        
        const svgContainer = document.getElementById('svgContainer');
        const svg = document.getElementById('treeSvg');
        const tooltip = document.getElementById('tooltip');
        
        let scale = 0.8;
        
        const NODE_WIDTH = 140;
        const NODE_HEIGHT = 36;
        const LEVEL_HEIGHT = 90;
        const NODE_SPACING = 25;
        const DIAMOND_SIZE = 24;
        
        const colors = {{
            'root': '#4caf50',
            'claim_type': '#9c27b0',
            'decision': '#2196f3',
            'branch_yes': '#4caf50',
            'branch_no': '#f44336',
            'branch_unsure': '#ff9800',
            'action': '#607d8b',
            'step': '#607d8b',
            'reference': '#e91e63',
            'sub_decision': '#03a9f4',
            'terminal': '#795548'
        }};
        
        function calculateTreeLayout(node, level, leftOffset) {{
            if (!node) return {{ width: 0, nodes: [], edges: [], depth: 0 }};
            
            const children = node.children || [];
            let nodes = [];
            let edges = [];
            let maxDepth = level;
            
            if (children.length === 0) {{
                const x = leftOffset + NODE_WIDTH / 2;
                const y = level * LEVEL_HEIGHT + 50;
                nodes.push({{ ...node, x, y }});
                return {{ width: NODE_WIDTH + NODE_SPACING, nodes, edges, depth: level }};
            }}
            
            let currentOffset = leftOffset;
            let childResults = [];
            
            for (const child of children) {{
                const result = calculateTreeLayout(child, level + 1, currentOffset);
                childResults.push(result);
                nodes = nodes.concat(result.nodes);
                edges = edges.concat(result.edges);
                currentOffset += result.width;
                maxDepth = Math.max(maxDepth, result.depth);
            }}
            
            const totalWidth = childResults.reduce((sum, r) => sum + r.width, 0);
            const firstChildX = childResults[0].nodes[0]?.x || leftOffset;
            const lastChildX = childResults[childResults.length - 1].nodes[0]?.x || leftOffset;
            const centerX = (firstChildX + lastChildX) / 2;
            const y = level * LEVEL_HEIGHT + 50;
            
            nodes.unshift({{ ...node, x: centerX, y }});
            
            for (const child of children) {{
                const childNode = nodes.find(n => n.id === child.id);
                if (childNode) {{
                    edges.push({{
                        fromX: centerX,
                        fromY: y,
                        toX: childNode.x,
                        toY: childNode.y,
                        label: child.edgeLabel || '',
                        parentType: node.type
                    }});
                }}
            }}
            
            return {{ width: Math.max(totalWidth, NODE_WIDTH + NODE_SPACING), nodes, edges, depth: maxDepth }};
        }}
        
        function renderTree() {{
            const tree = treesData[currentClaimType];
            if (!tree) {{
                svg.innerHTML = '<text x="50" y="100" fill="#666" font-size="16">No decision tree data for this claim type</text>';
                return;
            }}
            
            const result = calculateTreeLayout(tree, 0, 80);
            const {{ nodes, edges, depth }} = result;
            
            // Update stats
            document.getElementById('nodeCount').textContent = nodes.length;
            document.getElementById('treeDepth').textContent = depth;
            
            // Calculate SVG size
            let maxX = 0, maxY = 0;
            nodes.forEach(n => {{
                maxX = Math.max(maxX, n.x + NODE_WIDTH);
                maxY = Math.max(maxY, n.y + NODE_HEIGHT + 50);
            }});
            
            const width = Math.max(maxX + 100, 800);
            const height = Math.max(maxY + 80, 600);
            
            svg.setAttribute('width', width * scale);
            svg.setAttribute('height', height * scale);
            svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`);
            
            let svgContent = `
                <defs>
                    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
                        <feDropShadow dx="2" dy="2" stdDeviation="3" flood-opacity="0.2"/>
                    </filter>
                    <linearGradient id="edgeGradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" style="stop-color:#999;stop-opacity:0.8"/>
                        <stop offset="100%" style="stop-color:#999;stop-opacity:0.4"/>
                    </linearGradient>
                </defs>
            `;
            
            // Draw edges first
            edges.forEach(edge => {{
                const isDecision = edge.parentType === 'decision' || edge.parentType === 'sub_decision';
                const fromYOffset = edge.fromY + (isDecision ? DIAMOND_SIZE/2 + 8 : NODE_HEIGHT/2);
                const toYOffset = edge.toY - NODE_HEIGHT/2 - 3;
                const midY = (fromYOffset + toYOffset) / 2;
                
                // Curved path
                svgContent += `<path d="M ${{edge.fromX}} ${{fromYOffset}} C ${{edge.fromX}} ${{midY}}, ${{edge.toX}} ${{midY}}, ${{edge.toX}} ${{toYOffset}}" fill="none" stroke="url(#edgeGradient)" stroke-width="2"/>`;
                
                // Edge label
                if (edge.label) {{
                    const labelX = (edge.fromX + edge.toX) / 2;
                    const labelY = midY - 5;
                    const labelColor = edge.label === 'YES' ? '#4caf50' : edge.label === 'NO' ? '#f44336' : '#ff9800';
                    svgContent += `<text x="${{labelX}}" y="${{labelY}}" text-anchor="middle" font-size="10" font-weight="600" fill="${{labelColor}}">${{edge.label}}</text>`;
                }}
            }});
            
            // Draw nodes
            nodes.forEach(node => {{
                const color = colors[node.type] || colors.action;
                const isDecision = node.type === 'decision' || node.type === 'sub_decision';
                const escapedContent = (node.fullContent || node.name || '').replace(/'/g, "\\'").replace(/"/g, '\\"');
                
                if (isDecision) {{
                    // Diamond shape for decisions
                    const size = DIAMOND_SIZE;
                    svgContent += `<g class="tree-node" style="cursor:pointer" onmouseover="showTooltip(event, '${{node.type}}', '${{escapedContent}}')" onmouseout="hideTooltip()">`;
                    svgContent += `<rect x="${{node.x - size/2}}" y="${{node.y - size/2}}" width="${{size}}" height="${{size}}" fill="${{color}}" stroke="white" stroke-width="2" rx="4" transform="rotate(45 ${{node.x}} ${{node.y}})" filter="url(#shadow)"/>`;
                    const label = node.name.length > 24 ? node.name.substring(0, 24) + '...' : node.name;
                    svgContent += `<text x="${{node.x}}" y="${{node.y + size/2 + 16}}" text-anchor="middle" font-size="10" fill="#333" font-weight="500">${{label}}</text>`;
                    svgContent += '</g>';
                }} else {{
                    // Rounded rectangle for other nodes
                    svgContent += `<g class="tree-node" style="cursor:pointer" onmouseover="showTooltip(event, '${{node.type}}', '${{escapedContent}}')" onmouseout="hideTooltip()">`;
                    svgContent += `<rect x="${{node.x - NODE_WIDTH/2}}" y="${{node.y - NODE_HEIGHT/2}}" width="${{NODE_WIDTH}}" height="${{NODE_HEIGHT}}" fill="${{color}}" stroke="white" stroke-width="2" rx="8" filter="url(#shadow)"/>`;
                    const label = node.name.length > 20 ? node.name.substring(0, 20) + '...' : node.name;
                    svgContent += `<text x="${{node.x}}" y="${{node.y}}" text-anchor="middle" dominant-baseline="middle" font-size="10" fill="white" font-weight="500">${{label}}</text>`;
                    svgContent += '</g>';
                }}
            }});
            
            svg.innerHTML = svgContent;
        }}
        
        function showTooltip(event, type, content) {{
            const typeLabel = type.replace(/_/g, ' ');
            tooltip.innerHTML = '<strong>' + typeLabel + '</strong><br>' + content;
            tooltip.style.display = 'block';
            
            // Position tooltip
            let x = event.clientX + 15;
            let y = event.clientY + 15;
            
            // Keep tooltip on screen
            const rect = tooltip.getBoundingClientRect();
            if (x + 320 > window.innerWidth) x = event.clientX - 320;
            if (y + rect.height > window.innerHeight) y = event.clientY - rect.height - 15;
            
            tooltip.style.left = x + 'px';
            tooltip.style.top = y + 'px';
        }}
        
        function hideTooltip() {{
            tooltip.style.display = 'none';
        }}
        
        function showClaimType(claimType) {{
            currentClaimType = claimType;
            document.querySelectorAll('.claim-btn').forEach(btn => {{
                btn.classList.toggle('active', btn.textContent === claimType);
            }});
            resetView();
        }}
        
        function zoomIn() {{ 
            scale = Math.min(scale * 1.25, 2.5); 
            renderTree(); 
        }}
        
        function zoomOut() {{ 
            scale = Math.max(scale / 1.25, 0.3); 
            renderTree(); 
        }}
        
        function resetView() {{ 
            scale = 0.8; 
            svgContainer.scrollTo(0, 0); 
            renderTree(); 
        }}
        
        // Keyboard shortcuts
        document.addEventListener('keydown', e => {{
            if (e.key === '+' || e.key === '=') zoomIn();
            else if (e.key === '-') zoomOut();
            else if (e.key === '0') resetView();
        }});
        
        // Mouse wheel zoom
        svgContainer.addEventListener('wheel', e => {{
            if (e.ctrlKey) {{
                e.preventDefault();
                if (e.deltaY < 0) zoomIn();
                else zoomOut();
            }}
        }});
        
        // Initial render
        renderTree();
    </script>
</body>
</html>'''
        return html


# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class WorldNetworkProcessor:
    """Main processor class - orchestrates the entire pipeline"""
    
    def __init__(self):
        self.parser = SOPParser()
        self.builder = WorldNetworkBuilder()
    
    def process(self, pdf_path: str, output_dir: str):
        """Process a PDF and generate all outputs"""
        
        print("=" * 80)
        print("WORLD NETWORK BUILDER - Complete Processing Pipeline")
        print("=" * 80)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Step 1: Extract PDF content
        print("\n📄 Step 1: Extracting PDF content...")
        text = self._extract_pdf(pdf_path)
        print(f"   ✓ Extracted {len(text):,} characters")
        
        # Step 2: Parse the document
        print("\n📋 Step 2: Parsing document structure...")
        parsed_data = self.parser.parse(text)
        doc_info = parsed_data.get('document_info', {})
        document_id = doc_info.get('document_id', 'UNKNOWN')
        document_name = doc_info.get('title', 'Untitled Document')
        print(f"   ✓ Document: {document_name} ({document_id})")
        print(f"   ✓ Found {len(parsed_data.get('sections', []))} claim type sections")
        
        # Step 3: Build World Network
        print("\n🌐 Step 3: Building World Network...")
        network = self.builder.build(parsed_data, document_id, document_name)
        print(f"   ✓ Created {len(network.nodes)} nodes")
        print(f"   ✓ Created {len(network.edges)} edges")
        print(f"   ✓ Found {len(network.entities)} entities")
        print(f"   ✓ Found {len(network.procedure_refs)} procedure references")
        
        # Step 4: Generate outputs
        print("\n💾 Step 4: Generating outputs...")
        
        # Save World Network JSON
        json_path = os.path.join(output_dir, 'world_network.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(network.to_dict(), f, indent=2, ensure_ascii=False)
        print(f"   ✓ World Network JSON: {json_path}")
        
        # Save parsed data
        parsed_path = os.path.join(output_dir, 'parsed_sop.json')
        parsed_data_copy = {k: v for k, v in parsed_data.items() if k != 'raw_text'}
        with open(parsed_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_data_copy, f, indent=2, ensure_ascii=False)
        print(f"   ✓ Parsed SOP JSON: {parsed_path}")
        
        # Generate and save HTML visualization
        html_path = os.path.join(output_dir, 'world_network_tree.html')
        html_content = TreeVisualizationGenerator.generate(network)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"   ✓ Interactive HTML Tree: {html_path}")
        
        # Generate decision tree text
        tree_path = os.path.join(output_dir, 'decision_tree.txt')
        tree_text = self._generate_decision_tree_text(network)
        with open(tree_path, 'w', encoding='utf-8') as f:
            f.write(tree_text)
        print(f"   ✓ Decision Tree TXT: {tree_path}")
        
        # Generate statistics
        stats = self._generate_statistics(network)
        stats_path = os.path.join(output_dir, 'statistics.json')
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        print(f"   ✓ Statistics JSON: {stats_path}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("✅ PROCESSING COMPLETE")
        print("=" * 80)
        print(f"\n📊 SUMMARY:")
        print(f"   Document: {document_name} ({document_id})")
        print(f"   Version: {network.current_version}")
        print(f"   Nodes: {len(network.nodes)}")
        print(f"   Edges: {len(network.edges)}")
        print(f"   Claim Types: {len(network.claim_type_roots)}")
        for ct_name in network.claim_type_roots.keys():
            print(f"      • {ct_name}")
        print(f"\n📁 Output Directory: {output_dir}")
        
        return network
    
    def _extract_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using pymupdf4llm"""
        try:
            import pymupdf4llm
            return pymupdf4llm.to_markdown(pdf_path)
        except ImportError:
            print("   ⚠️  pymupdf4llm not installed, trying PyMuPDF...")
            try:
                import fitz
                doc = fitz.open(pdf_path)
                text = ""
                for page in doc:
                    text += page.get_text()
                doc.close()
                return text
            except ImportError:
                raise ImportError("Please install pymupdf4llm: pip install pymupdf4llm")
    
    def _generate_decision_tree_text(self, network: WorldNetwork) -> str:
        """Generate text representation of decision tree"""
        lines = []
        lines.append("=" * 80)
        lines.append(f"DECISION TREE: {network.document_name}")
        lines.append(f"Document ID: {network.document_id}")
        lines.append(f"Version: {network.current_version}")
        lines.append("=" * 80)
        
        def format_node(node_id, indent=0):
            if node_id not in network.nodes:
                return []
            
            node = network.nodes[node_id]
            prefix = "  " * indent
            result = []
            
            # Format based on node type
            if node.node_type == NodeType.DECISION:
                result.append(f"{prefix}❓ [{node.step_number}] {node.content}")
            elif node.node_type == NodeType.BRANCH_YES:
                result.append(f"{prefix}✅ YES: {node.content[:80]}...")
            elif node.node_type == NodeType.BRANCH_NO:
                result.append(f"{prefix}❌ NO: {node.content[:80]}...")
            elif node.node_type == NodeType.BRANCH_UNSURE:
                result.append(f"{prefix}❓ UNSURE: {node.content[:80]}...")
            elif node.node_type == NodeType.REFERENCE:
                result.append(f"{prefix}🔗 {node.content}")
            elif node.node_type == NodeType.CLAIM_TYPE:
                result.append(f"\n{prefix}📋 {node.content}")
                result.append(f"{prefix}" + "-" * 40)
            else:
                result.append(f"{prefix}• {node.content[:80]}")
            
            # Process children
            for edge in network.get_outgoing_edges(node_id):
                result.extend(format_node(edge.target_id, indent + 1))
            
            return result
        
        # Format each claim type
        for claim_type, root_id in network.claim_type_roots.items():
            lines.extend(format_node(root_id))
        
        return "\n".join(lines)
    
    def _generate_statistics(self, network: WorldNetwork) -> Dict:
        """Generate statistics about the network"""
        node_types = defaultdict(int)
        for node in network.nodes.values():
            node_types[node.node_type.value] += 1
        
        entity_types = defaultdict(int)
        for entity in network.entities.values():
            entity_types[entity.entity_type] += 1
        
        # Calculate max depth per claim type
        def calc_depth(node_id, visited=None):
            if visited is None:
                visited = set()
            if node_id in visited or node_id not in network.nodes:
                return 0
            visited.add(node_id)
            
            max_child_depth = 0
            for edge in network.get_outgoing_edges(node_id):
                child_depth = calc_depth(edge.target_id, visited.copy())
                max_child_depth = max(max_child_depth, child_depth)
            
            return 1 + max_child_depth
        
        claim_type_depths = {}
        for ct_name, root_id in network.claim_type_roots.items():
            claim_type_depths[ct_name] = calc_depth(root_id)
        
        return {
            'document_id': network.document_id,
            'document_name': network.document_name,
            'version': network.current_version,
            'total_nodes': len(network.nodes),
            'total_edges': len(network.edges),
            'total_entities': len(network.entities),
            'total_procedure_refs': len(network.procedure_refs),
            'node_types': dict(node_types),
            'entity_types': dict(entity_types),
            'claim_types': list(network.claim_type_roots.keys()),
            'claim_type_depths': claim_type_depths
        }


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    if len(sys.argv) < 3:
        print("Usage: python world_network_complete.py <pdf_path> <output_dir>")
        print("\nExample:")
        print("  python world_network_complete.py ./P966.pdf ./output")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found: {pdf_path}")
        sys.exit(1)
    
    processor = WorldNetworkProcessor()
    processor.process(pdf_path, output_dir)


if __name__ == "__main__":
    main()
