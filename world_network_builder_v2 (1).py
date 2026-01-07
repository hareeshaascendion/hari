"""
Phase 1: World Network Builder v2.0
====================================
Transforms SOP/DLP documents into structured, versioned, deterministic executable graphs.

Key Components:
1. World Network (WN) - Deterministic flow graph representing objective truth of claims processing
2. Observation Network (ON) - Core entities and prior knowledge extraction (Premera-wide)
3. Deep Link Resolution - Recursive crawling of linked procedures (PR.OP.CL.*)
4. Version Control - Maintains distinct graph versions (Rev 1.0, 1.1, etc.)

Architecture:
- This is NOT a knowledge graph, state machine, or flowchart
- It IS a deterministic flow graph - a super-logical decision tree
- Logic exists in unstructured text, converted to structured nodes
- Each claim type (Amazon, Microsoft, etc.) gets its own subgraph
"""

import re
import json
import hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from enum import Enum
from collections import defaultdict
import uuid


# ============================================================================
# ENUMS AND CONSTANTS
# ============================================================================

class NodeType(Enum):
    """Types of nodes in the World Network"""
    ROOT = "root"                      # Document root
    CLAIM_TYPE = "claim_type"          # Major claim category (Amazon, Microsoft, etc.)
    DECISION = "decision"              # Decision point (Yes/No question)
    CONDITION = "condition"            # Conditional check within a step
    ACTION = "action"                  # Terminal action to perform
    BRANCH_YES = "branch_yes"          # Yes branch
    BRANCH_NO = "branch_no"            # No branch
    BRANCH_UNSURE = "branch_unsure"    # Unsure branch
    SUB_DECISION = "sub_decision"      # Nested decision within a branch
    REFERENCE = "reference"            # Reference to another procedure
    TABLE = "table"                    # Table data (lookups, clinics)
    NOTE = "note"                      # Important note
    TERMINAL = "terminal"              # End state
    STEP = "step"                      # Sequential step (non-decision)
    LOOKUP_TABLE = "lookup_table"      # Provider/clinic lookup table


class EdgeType(Enum):
    """Types of edges in the World Network"""
    SEQUENCE = "sequence"                   # Sequential flow
    CONDITION_YES = "condition_yes"         # Yes branch
    CONDITION_NO = "condition_no"           # No branch  
    CONDITION_UNSURE = "condition_unsure"   # Unsure branch
    NESTED_YES = "nested_yes"               # Nested yes within branch
    NESTED_NO = "nested_no"                 # Nested no within branch
    REFERENCE = "reference"                 # Link to another procedure
    CONTAINS = "contains"                   # Parent contains child
    CONTINUE_TO_STEP = "continue_to_step"   # Continue to next step
    PROCEED_TO_SECTION = "proceed_to_section"  # Jump to another section
    LOOKUP = "lookup"                       # Table lookup edge


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
class Node:
    """A node in the World Network"""
    id: str
    node_type: NodeType
    content: str
    raw_text: str = ""
    step_number: Optional[int] = None
    parent_id: Optional[str] = None
    section: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    entities: List[str] = field(default_factory=list)
    position: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['node_type'] = self.node_type.value
        return d


@dataclass
class Edge:
    """An edge in the World Network"""
    id: str
    source_id: str
    target_id: str
    edge_type: EdgeType
    condition: Optional[str] = None
    condition_value: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        d = asdict(self)
        d['edge_type'] = self.edge_type.value
        return d


@dataclass
class ProcedureReference:
    """Reference to another procedure for deep linking"""
    id: str
    procedure_code: str
    procedure_name: str
    url: Optional[str] = None
    resolved: bool = False
    source_node_id: Optional[str] = None
    source_context: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Version:
    """Version information for document"""
    revision: str
    date: str
    description: str
    content_hash: str
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ClinicEntry:
    """Clinic/Provider lookup entry"""
    name: str
    tin: Optional[str] = None
    provider_id: Optional[str] = None
    npi: Optional[str] = None
    location: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


# ============================================================================
# WORLD NETWORK GRAPH
# ============================================================================

class WorldNetwork:
    """
    The World Network - Deterministic flow graph representing SOP logic.
    One network per document, with subgraphs for each claim type.
    """
    
    def __init__(self, document_id: str, document_name: str):
        self.document_id = document_id
        self.document_name = document_name
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[str, Edge] = {}
        self.root_id: Optional[str] = None
        self.versions: List[Version] = []
        self.current_version: Optional[str] = None
        self.procedure_refs: Dict[str, ProcedureReference] = {}
        self.entities: Dict[str, Entity] = {}
        self.claim_type_roots: Dict[str, str] = {}  # Map claim_type -> root_node_id
        self.lookup_tables: Dict[str, List[ClinicEntry]] = {}
        self.metadata: Dict[str, Any] = {
            'created_at': datetime.now().isoformat(),
            'source_type': 'SOP',
            'builder_version': '2.0'
        }
    
    def add_node(self, node: Node) -> str:
        """Add a node to the network"""
        self.nodes[node.id] = node
        if node.node_type == NodeType.ROOT:
            self.root_id = node.id
        elif node.node_type == NodeType.CLAIM_TYPE:
            self.claim_type_roots[node.content] = node.id
        return node.id
    
    def add_edge(self, edge: Edge) -> str:
        """Add an edge to the network"""
        self.edges[edge.id] = edge
        return edge.id
    
    def get_children(self, node_id: str) -> List[Node]:
        """Get all direct children of a node"""
        children = []
        for edge in self.edges.values():
            if edge.source_id == node_id:
                if edge.target_id in self.nodes:
                    children.append(self.nodes[edge.target_id])
        return children
    
    def get_outgoing_edges(self, node_id: str) -> List[Edge]:
        """Get all outgoing edges from a node"""
        return [e for e in self.edges.values() if e.source_id == node_id]
    
    def get_incoming_edges(self, node_id: str) -> List[Edge]:
        """Get all incoming edges to a node"""
        return [e for e in self.edges.values() if e.target_id == node_id]
    
    def get_claim_type_graph(self, claim_type: str) -> Dict:
        """Extract subgraph for a specific claim type"""
        if claim_type not in self.claim_type_roots:
            return {}
        
        root_id = self.claim_type_roots[claim_type]
        visited = set()
        nodes = {}
        edges = {}
        
        def traverse(node_id):
            if node_id in visited:
                return
            visited.add(node_id)
            
            if node_id in self.nodes:
                nodes[node_id] = self.nodes[node_id]
                
            for edge in self.get_outgoing_edges(node_id):
                edges[edge.id] = edge
                traverse(edge.target_id)
        
        traverse(root_id)
        
        return {
            'claim_type': claim_type,
            'root_id': root_id,
            'nodes': {k: v.to_dict() for k, v in nodes.items()},
            'edges': {k: v.to_dict() for k, v in edges.items()}
        }
    
    def get_decision_path(self, start_node_id: str, conditions: Dict[str, bool]) -> List[str]:
        """
        Traverse the decision tree given a set of conditions.
        Returns the path of node IDs.
        """
        path = []
        current = start_node_id
        
        while current:
            path.append(current)
            node = self.nodes.get(current)
            
            if not node:
                break
            
            # Get outgoing edges
            outgoing = self.get_outgoing_edges(current)
            
            if not outgoing:
                break
            
            # Find the appropriate next node based on conditions
            next_node = None
            for edge in outgoing:
                if edge.edge_type == EdgeType.SEQUENCE:
                    next_node = edge.target_id
                    break
                elif edge.condition:
                    condition_key = f"{current}_{edge.condition}"
                    if condition_key in conditions:
                        if conditions[condition_key]:
                            next_node = edge.target_id
                            break
            
            current = next_node
        
        return path
    
    def to_dict(self) -> Dict:
        """Export network to dictionary"""
        return {
            'document_id': self.document_id,
            'document_name': self.document_name,
            'root_id': self.root_id,
            'current_version': self.current_version,
            'metadata': self.metadata,
            'claim_type_roots': self.claim_type_roots,
            'nodes': {k: v.to_dict() for k, v in self.nodes.items()},
            'edges': {k: v.to_dict() for k, v in self.edges.items()},
            'versions': [v.to_dict() for v in self.versions],
            'procedure_refs': {k: v.to_dict() for k, v in self.procedure_refs.items()},
            'entities': {k: v.to_dict() for k, v in self.entities.items()},
            'lookup_tables': {k: [e.to_dict() for e in v] for k, v in self.lookup_tables.items()}
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Export network to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ============================================================================
# OBSERVATION NETWORK
# ============================================================================

class ObservationNetwork:
    """
    Observation Network - Extracts and maintains prior knowledge about entities.
    This is Premera-wide, not specific to one SOP.
    """
    
    def __init__(self):
        self.entities: Dict[str, Entity] = {}
        self.relationships: List[Dict] = []
        self.categories: Dict[str, Set[str]] = defaultdict(set)
        self.provider_lookup: Dict[str, List[str]] = defaultdict(list)  # TIN -> [provider_ids]
        self.clinic_directory: Dict[str, ClinicEntry] = {}
    
    def add_entity(self, entity: Entity):
        """Add or merge an entity"""
        if entity.id in self.entities:
            # Merge mentions
            self.entities[entity.id].mentions.extend(entity.mentions)
        else:
            self.entities[entity.id] = entity
        
        # Categorize
        self.categories[entity.entity_type].add(entity.id)
        
        # Build lookup indexes
        if entity.entity_type == EntityType.PROVIDER_ID.value:
            tin = entity.attributes.get('tin')
            if tin:
                self.provider_lookup[tin].append(entity.id)
    
    def add_relationship(self, entity1_id: str, entity2_id: str, relationship_type: str, context: str = ""):
        """Add a relationship between entities"""
        rel = {
            'source': entity1_id,
            'target': entity2_id,
            'type': relationship_type,
            'context': context
        }
        self.relationships.append(rel)
        
        # Update entity relationships
        if entity1_id in self.entities:
            self.entities[entity1_id].relationships.append(rel)
    
    def absorb_from_world_network(self, network: WorldNetwork):
        """Extract entities from a World Network"""
        for entity_id, entity in network.entities.items():
            self.add_entity(entity)
        
        # Extract clinic entries
        for table_name, entries in network.lookup_tables.items():
            for entry in entries:
                key = f"{entry.name}_{entry.tin or ''}"
                self.clinic_directory[key] = entry
    
    def get_providers_by_tin(self, tin: str) -> List[Entity]:
        """Get all providers associated with a TIN"""
        provider_ids = self.provider_lookup.get(tin, [])
        return [self.entities[pid] for pid in provider_ids if pid in self.entities]
    
    def get_entity_summary(self) -> Dict:
        """Get summary statistics"""
        return {
            'total_entities': len(self.entities),
            'by_category': {k: len(v) for k, v in self.categories.items()},
            'total_relationships': len(self.relationships),
            'unique_tins': len(self.provider_lookup),
            'clinic_entries': len(self.clinic_directory)
        }
    
    def to_dict(self) -> Dict:
        """Export to dictionary"""
        return {
            'entities': {k: v.to_dict() for k, v in self.entities.items()},
            'categories': {k: list(v) for k, v in self.categories.items()},
            'relationships': self.relationships,
            'provider_lookup': dict(self.provider_lookup),
            'clinic_directory': {k: v.to_dict() for k, v in self.clinic_directory.items()},
            'summary': self.get_entity_summary()
        }


# ============================================================================
# SOP PARSER - Enhanced
# ============================================================================

class SOPParserV2:
    """
    Enhanced SOP Parser - Extracts structured decision tree from markdown.
    Handles nested decisions, lookup tables, and complex branching.
    """
    
    # Regex patterns
    PATTERNS = {
        # Headers and sections
        'main_section': re.compile(r'^###\s+\*\*(.+?)\*\*\s*$', re.MULTILINE),
        'section_header': re.compile(r'^##\s+\*\*_?(.+?)_?\*\*\s*$', re.MULTILINE),
        
        # Steps
        'numbered_step': re.compile(r'^(\d+)\.\s+(.+?)(?=^\d+\.|^###|\Z)', re.MULTILINE | re.DOTALL),
        
        # Branches
        'yes_branch': re.compile(r'^\s*[-–]\s*\*\*Yes:\*\*\s*(.+?)(?=^\s*[-–]\s*\*\*(?:Yes|No|Unsure):|^\d+\.|^###|\Z)', 
                                 re.MULTILINE | re.DOTALL | re.IGNORECASE),
        'no_branch': re.compile(r'^\s*[-–]\s*\*\*No:\*\*\s*(.+?)(?=^\s*[-–]\s*\*\*(?:Yes|No|Unsure):|^\d+\.|^###|\Z)', 
                                re.MULTILINE | re.DOTALL | re.IGNORECASE),
        'unsure_branch': re.compile(r'^\s*[-–]\s*\*\*Unsure:\*\*\s*(.+?)(?=^\s*[-–]\s*\*\*(?:Yes|No|Unsure):|^\d+\.|^###|\Z)', 
                                    re.MULTILINE | re.DOTALL | re.IGNORECASE),
        
        # Nested branches (using "I" bullet style from PDF conversion)
        'nested_yes': re.compile(r'^\s*I\s*\*\*Yes:\*\*\s*(.+?)(?=^\s*I?\s*\*\*(?:Yes|No):|^\s*[-–]|\Z)', 
                                 re.MULTILINE | re.DOTALL | re.IGNORECASE),
        'nested_no': re.compile(r'^\s*I\s*\*\*No:\*\*\s*(.+?)(?=^\s*I?\s*\*\*(?:Yes|No):|^\s*[-–]|\Z)', 
                                re.MULTILINE | re.DOTALL | re.IGNORECASE),
        
        # Sub-items and sub-scenarios
        'sub_item': re.compile(r'^\s*[-–I]\s*\*\*(.+?):\*\*\s*(.+?)(?=^\s*[-–I]\s*\*\*|^\d+\.|^###|\Z)', 
                               re.MULTILINE | re.DOTALL),
        
        # Entities
        'procedure_ref': re.compile(r'(PR\.OP\.CL\.\d+)(?:\s*[-–]\s*([^.\n]+))?', re.IGNORECASE),
        'provider_id': re.compile(r'\b([A-Z]\d{2}[A-Z0-9]{2,4}[A-Z]\d{2}[A-Z0-9]{2,4})\b'),
        'tin': re.compile(r'(?:TIN|tax identification number)[:\s]*(\d{9}|\d{3}-?\d{2}-?\d{4})', re.IGNORECASE),
        'tin_standalone': re.compile(r'\b(\d{9})\b'),
        'npi': re.compile(r'(?:NPI)[:\s]*(\d{10})'),
        'pend_code': re.compile(r'(?:pend(?:\s+(?:code|to))?)[:\s]*([A-Z]{1,2}\d{2,3})', re.IGNORECASE),
        'pca': re.compile(r'(?:PCA)[:\s]*([A-Z]?\d{3,4})', re.IGNORECASE),
        'group_number': re.compile(r'(?:group(?:\s+number)?)[:\s]*(\d{7})', re.IGNORECASE),
        'ultra_blue': re.compile(r'Ultra Blue message\s+([A-Z]{2,4}\s*[-–]\s*[A-Z\s]+?)(?=\.|,|\s+on)', re.IGNORECASE),
        
        # Tables (markdown format)
        'table': re.compile(r'\|.+\|[\s\S]*?\|.+\|', re.MULTILINE),
        
        # Notes
        'important_note': re.compile(r'\*\*Important Note:(.+?)\*\*', re.DOTALL | re.IGNORECASE),
        
        # Revision history
        'revision_entry': re.compile(r'\|(\d+\.\d+)\|([^|]+)\|([^|]+)\|'),
        
        # Provider names (known list)
        'provider_names': [
            'Vita Health', 'Concentra', 'Crossover', 'MedAire', 'Omada', 
            'Physera', 'Kabafusion', 'Progyny', '98POINT6', 'VSP Retail',
            'UPMC', 'Regenexx', 'Care Medical', 'Crossover Health'
        ],
        
        # Clinic patterns (from tables)
        'clinic_pattern': re.compile(r'(Crossover\s+\w+|Care Medical\s+\w+)', re.IGNORECASE)
    }
    
    def __init__(self):
        self.entities_found: Dict[str, Entity] = {}
        self.procedure_refs_found: Dict[str, ProcedureReference] = {}
        self.lookup_tables_found: Dict[str, List[ClinicEntry]] = {}
    
    def parse(self, markdown_content: str, document_id: str = None) -> Dict:
        """Parse SOP markdown into structured data"""
        if not document_id:
            document_id = hashlib.md5(markdown_content.encode()).hexdigest()[:12]
        
        result = {
            'document_id': document_id,
            'title': '',
            'document_type': '',
            'document_number': '',
            'status': '',
            'cause_explanation': '',
            'claim_types': [],  # Each claim type gets its own structure
            'revision_history': [],
            'metadata': {},
            'entities': {},
            'procedure_refs': {},
            'lookup_tables': {}
        }
        
        # Extract header info
        result.update(self._extract_header(markdown_content))
        
        # Extract revision history
        result['revision_history'] = self._extract_revision_history(markdown_content)
        
        # Extract claim type sections
        result['claim_types'] = self._extract_claim_types(markdown_content)
        
        # Extract lookup tables
        result['lookup_tables'] = self._extract_lookup_tables(markdown_content)
        self.lookup_tables_found = result['lookup_tables']
        
        # Compile all entities
        result['entities'] = {k: v.to_dict() for k, v in self.entities_found.items()}
        result['procedure_refs'] = {k: v.to_dict() for k, v in self.procedure_refs_found.items()}
        
        return result
    
    def _extract_header(self, content: str) -> Dict:
        """Extract document header information"""
        header = {}
        
        # Title
        title_match = re.search(r'^#\s+\*\*(.+?)\*\*', content, re.MULTILINE)
        if title_match:
            header['title'] = title_match.group(1).strip()
        
        # Document Type and Number
        doc_match = re.search(r'\*\*Document Type:\*\*\s*(\w+)\s*\*\*Document Number:\*\*\s*(\w+)', content)
        if doc_match:
            header['document_type'] = doc_match.group(1)
            header['document_number'] = doc_match.group(2)
        
        # Status
        status_match = re.search(r'\*\*Status:\*\*\s*([^\n]+)', content)
        if status_match:
            header['status'] = status_match.group(1).strip()
        
        # Cause/Explanation
        cause_match = re.search(r'\*\*Cause/Explanation:\*\*\s*\n\n?([^\n]+(?:\n[^\n*#]+)*)', content)
        if cause_match:
            header['cause_explanation'] = cause_match.group(1).strip()
        
        # Pend Code
        pend_match = re.search(r'\*\*Pend Code:\*\*\s*\n\n?([A-Z0-9]+)', content)
        if pend_match:
            pend_code = pend_match.group(1).strip()
            header['pend_code'] = pend_code
            self._register_entity(
                f"pend_{pend_code}",
                pend_code,
                EntityType.PEND_CODE.value,
                {'document_pend_code': True}
            )
        
        return header
    
    def _extract_revision_history(self, content: str) -> List[Dict]:
        """Extract revision history"""
        revisions = []
        for match in self.PATTERNS['revision_entry'].finditer(content):
            revisions.append({
                'revision': match.group(1),
                'date': match.group(2).strip(),
                'description': match.group(3).strip()[:500]
            })
        return revisions
    
    def _extract_claim_types(self, content: str) -> List[Dict]:
        """Extract each claim type section as a separate decision tree"""
        claim_types = []
        
        # Find the "Action Required" section
        action_match = re.search(r'##\s+\*\*_Action Required_\*\*', content)
        if action_match:
            content = content[action_match.end():]
        
        # Find all main sections (claim types)
        sections = list(self.PATTERNS['main_section'].finditer(content))
        
        for i, match in enumerate(sections):
            section_name = match.group(1).strip()
            start_pos = match.end()
            end_pos = sections[i + 1].start() if i + 1 < len(sections) else len(content)
            
            section_content = content[start_pos:end_pos]
            
            # Skip "Overview" type sections
            if 'Overview' in section_name or not section_content.strip():
                continue
            
            # Parse the section into steps and decisions
            claim_type = {
                'name': section_name,
                'steps': self._parse_steps(section_content, section_name),
                'raw_length': len(section_content)
            }
            
            claim_types.append(claim_type)
        
        return claim_types
    
    def _parse_steps(self, content: str, section_name: str) -> List[Dict]:
        """Parse numbered steps within a section"""
        steps = []
        
        # Find all numbered steps
        step_pattern = re.compile(r'^(\d+)\.\s+(.+?)(?=^\d+\.\s+|\Z)', re.MULTILINE | re.DOTALL)
        
        for match in step_pattern.finditer(content):
            step_num = int(match.group(1))
            step_content = match.group(2).strip()
            
            step = self._parse_single_step(step_num, step_content, section_name)
            steps.append(step)
        
        return steps
    
    def _parse_single_step(self, step_num: int, content: str, section_name: str) -> Dict:
        """Parse a single step with its decision branches"""
        step = {
            'step_number': step_num,
            'question': '',
            'is_decision': False,
            'branches': [],
            'nested_decisions': [],
            'notes': [],
            'entities': [],
            'procedure_refs': [],
            'raw_content': content[:1000]
        }
        
        # Extract the main question (text before first branch)
        lines = content.split('\n')
        question_lines = []
        branch_start_idx = len(lines)
        
        for i, line in enumerate(lines):
            if re.match(r'^\s*[-–]\s*\*\*(Yes|No|Unsure):', line, re.IGNORECASE):
                branch_start_idx = i
                break
            question_lines.append(line)
        
        step['question'] = ' '.join(question_lines).strip()
        step['question'] = re.sub(r'\s+', ' ', step['question'])  # Normalize whitespace
        
        # Check if this is a decision (has Yes/No branches)
        branch_content = '\n'.join(lines[branch_start_idx:])
        has_yes = bool(re.search(r'\*\*Yes:', branch_content, re.IGNORECASE))
        has_no = bool(re.search(r'\*\*No:', branch_content, re.IGNORECASE))
        
        step['is_decision'] = has_yes or has_no
        
        if step['is_decision']:
            step['branches'] = self._parse_branches(branch_content, section_name, step_num)
        
        # Extract notes
        for note_match in self.PATTERNS['important_note'].finditer(content):
            step['notes'].append(note_match.group(1).strip())
        
        # Extract entities
        step['entities'] = self._extract_entities(content, section_name, step_num)
        
        # Extract procedure references
        step['procedure_refs'] = self._extract_procedure_refs(content, section_name, step_num)
        
        return step
    
    def _parse_branches(self, content: str, section_name: str, step_num: int) -> List[Dict]:
        """Parse Yes/No/Unsure branches with nested decisions"""
        branches = []
        
        # Parse Yes branch
        yes_match = self.PATTERNS['yes_branch'].search(content)
        if yes_match:
            branches.append(self._parse_single_branch('yes', yes_match.group(1), section_name, step_num))
        
        # Parse No branch
        no_match = self.PATTERNS['no_branch'].search(content)
        if no_match:
            branches.append(self._parse_single_branch('no', no_match.group(1), section_name, step_num))
        
        # Parse Unsure branch
        unsure_match = self.PATTERNS['unsure_branch'].search(content)
        if unsure_match:
            branches.append(self._parse_single_branch('unsure', unsure_match.group(1), section_name, step_num))
        
        return branches
    
    def _parse_single_branch(self, branch_type: str, content: str, section_name: str, step_num: int) -> Dict:
        """Parse a single branch and its nested decisions"""
        branch = {
            'type': branch_type,
            'action': '',
            'nested_decisions': [],
            'sub_scenarios': [],
            'entities': [],
            'procedure_refs': [],
            'terminal_action': None,
            'continue_to_step': None,
            'proceed_to_section': None
        }
        
        # Check for "Continue to next step"
        if re.search(r'continue\s+to\s+(the\s+)?next\s+step', content, re.IGNORECASE):
            branch['continue_to_step'] = True
            branch['action'] = content.strip()[:500]
            return branch
        
        # Check for "Proceed to section"
        proceed_match = re.search(r'proceed\s+to\s+(the\s+)?(.+?)\s+section', content, re.IGNORECASE)
        if proceed_match:
            branch['proceed_to_section'] = proceed_match.group(2).strip()
        
        # Check for nested Yes/No decisions
        has_nested = bool(re.search(r'I\s*\*\*(?:Yes|No):', content, re.IGNORECASE))
        
        if has_nested:
            # Extract action before nested decisions
            first_nested = re.search(r'I\s*\*\*(?:Yes|No):', content, re.IGNORECASE)
            if first_nested:
                branch['action'] = content[:first_nested.start()].strip()[:500]
            
            # Parse nested decisions
            branch['nested_decisions'] = self._parse_nested_decisions(content, section_name, step_num)
        else:
            branch['action'] = content.strip()[:500]
        
        # Check for sub-scenarios (Provider-submitted, Member-submitted, etc.)
        sub_pattern = re.compile(r'[-–]\s*\*\*([^:]+?):\*\*\s*(.+?)(?=[-–]\s*\*\*[^:]+?:\*\*|$)', re.DOTALL)
        for sub_match in sub_pattern.finditer(content):
            scenario_type = sub_match.group(1).strip()
            scenario_content = sub_match.group(2).strip()
            
            # Skip Yes/No which are handled as branches
            if scenario_type.lower() in ['yes', 'no', 'unsure']:
                continue
            
            sub_scenario = {
                'type': scenario_type,
                'action': scenario_content[:500],
                'entities': self._extract_entities(scenario_content, section_name, step_num),
                'procedure_refs': self._extract_procedure_refs(scenario_content, section_name, step_num)
            }
            branch['sub_scenarios'].append(sub_scenario)
        
        # Extract entities
        branch['entities'] = self._extract_entities(content, section_name, step_num)
        branch['procedure_refs'] = self._extract_procedure_refs(content, section_name, step_num)
        
        return branch
    
    def _parse_nested_decisions(self, content: str, section_name: str, step_num: int) -> List[Dict]:
        """Parse nested Yes/No decisions within a branch"""
        nested = []
        
        # Find nested Yes
        nested_yes = self.PATTERNS['nested_yes'].search(content)
        if nested_yes:
            nested.append({
                'type': 'yes',
                'action': nested_yes.group(1).strip()[:500],
                'entities': self._extract_entities(nested_yes.group(1), section_name, step_num),
                'procedure_refs': self._extract_procedure_refs(nested_yes.group(1), section_name, step_num)
            })
        
        # Find nested No
        nested_no = self.PATTERNS['nested_no'].search(content)
        if nested_no:
            nested.append({
                'type': 'no',
                'action': nested_no.group(1).strip()[:500],
                'entities': self._extract_entities(nested_no.group(1), section_name, step_num),
                'procedure_refs': self._extract_procedure_refs(nested_no.group(1), section_name, step_num)
            })
        
        return nested
    
    def _extract_lookup_tables(self, content: str) -> Dict[str, List[ClinicEntry]]:
        """Extract lookup tables (clinics, providers) from markdown tables"""
        tables = {}
        
        # Find table blocks
        table_pattern = re.compile(r'\|(.+?)\|(.+?)\|(.+?)\|[\s\S]*?(?=\n\n|\n[^|]|\Z)')
        
        # Pattern for clinic entries (Crossover, Care Medical)
        crossover_pattern = re.compile(r'Crossover\s+(\w+).*?:\s*([A-Z]\d{2}[A-Z0-9]+)', re.IGNORECASE)
        care_medical_pattern = re.compile(r'Care Medical\s+(\w+).*?\|(\d{9})\|([A-Z]\d{2}[A-Z0-9]+)', re.IGNORECASE)
        
        # Extract Crossover clinics
        crossover_clinics = []
        for match in re.finditer(r'I?\s*Crossover\s+(\w+):\s*([A-Z]\d{2}[A-Z0-9]+|(?:NPI:?\s*)?\d{10})', content, re.IGNORECASE):
            location = match.group(1)
            identifier = match.group(2)
            
            entry = ClinicEntry(
                name=f"Crossover {location}",
                location=location
            )
            
            if len(identifier) == 10 and identifier.isdigit():
                entry.npi = identifier
            else:
                entry.provider_id = identifier
            
            crossover_clinics.append(entry)
            
            # Register entity
            if entry.provider_id:
                self._register_entity(
                    f"provider_{entry.provider_id}",
                    entry.provider_id,
                    EntityType.PROVIDER_ID.value,
                    {'clinic_name': entry.name, 'location': location}
                )
        
        if crossover_clinics:
            tables['crossover_clinics'] = crossover_clinics
        
        # Extract Care Medical clinics from table
        care_medical_clinics = []
        care_table_pattern = re.compile(r'\|Care Medical\s+(\w+)[^|]*\|(\d{9})\|([A-Z]\d{2}[A-Z0-9]+)\|')
        for match in care_table_pattern.finditer(content):
            state = match.group(1)
            tin = match.group(2)
            provider_id = match.group(3)
            
            entry = ClinicEntry(
                name=f"Care Medical {state}",
                tin=tin,
                provider_id=provider_id,
                location=state
            )
            care_medical_clinics.append(entry)
            
            # Register entities
            self._register_entity(
                f"provider_{provider_id}",
                provider_id,
                EntityType.PROVIDER_ID.value,
                {'clinic_name': entry.name, 'tin': tin}
            )
        
        if care_medical_clinics:
            tables['care_medical_clinics'] = care_medical_clinics
        
        return tables
    
    def _extract_entities(self, text: str, section_name: str, step_num: int) -> List[str]:
        """Extract and register entities from text"""
        entity_ids = []
        
        # Provider IDs
        for match in self.PATTERNS['provider_id'].finditer(text):
            pid = match.group(1)
            entity_id = f"provider_{pid}"
            self._register_entity(entity_id, pid, EntityType.PROVIDER_ID.value, {})
            self.entities_found[entity_id].mentions.append({
                'section': section_name,
                'step': step_num
            })
            entity_ids.append(entity_id)
        
        # TINs (with context)
        for match in self.PATTERNS['tin'].finditer(text):
            tin = match.group(1).replace('-', '')
            entity_id = f"tin_{tin}"
            self._register_entity(entity_id, tin, EntityType.TIN.value, {})
            entity_ids.append(entity_id)
        
        # NPIs
        for match in self.PATTERNS['npi'].finditer(text):
            npi = match.group(1)
            entity_id = f"npi_{npi}"
            self._register_entity(entity_id, npi, EntityType.NPI.value, {})
            entity_ids.append(entity_id)
        
        # Pend codes
        for match in self.PATTERNS['pend_code'].finditer(text):
            code = match.group(1).upper()
            entity_id = f"pend_{code}"
            self._register_entity(entity_id, code, EntityType.PEND_CODE.value, {})
            entity_ids.append(entity_id)
        
        # PCA codes
        for match in self.PATTERNS['pca'].finditer(text):
            code = match.group(1).upper()
            entity_id = f"pca_{code}"
            self._register_entity(entity_id, code, EntityType.PCA_CODE.value, {})
            entity_ids.append(entity_id)
        
        # Group numbers
        for match in self.PATTERNS['group_number'].finditer(text):
            group = match.group(1)
            entity_id = f"group_{group}"
            self._register_entity(entity_id, group, EntityType.GROUP_NUMBER.value, {})
            entity_ids.append(entity_id)
        
        # Ultra Blue messages
        for match in self.PATTERNS['ultra_blue'].finditer(text):
            msg = match.group(1).strip()
            entity_id = f"ub_{msg.replace(' ', '_').replace('-', '_')}"
            self._register_entity(entity_id, msg, EntityType.ULTRA_BLUE_MESSAGE.value, {})
            entity_ids.append(entity_id)
        
        # Provider names
        for provider in self.PATTERNS['provider_names']:
            if provider.lower() in text.lower():
                entity_id = f"provider_name_{provider.replace(' ', '_').lower()}"
                self._register_entity(entity_id, provider, EntityType.PROVIDER_NAME.value, {})
                entity_ids.append(entity_id)
        
        return list(set(entity_ids))
    
    def _extract_procedure_refs(self, text: str, section_name: str, step_num: int) -> List[str]:
        """Extract procedure references"""
        ref_ids = []
        
        for match in self.PATTERNS['procedure_ref'].finditer(text):
            proc_code = match.group(1).upper()
            proc_name = match.group(2).strip() if match.group(2) else ''
            
            ref_id = f"ref_{proc_code.replace('.', '_')}"
            
            if ref_id not in self.procedure_refs_found:
                self.procedure_refs_found[ref_id] = ProcedureReference(
                    id=ref_id,
                    procedure_code=proc_code,
                    procedure_name=proc_name,
                    source_context=f"{section_name} Step {step_num}"
                )
            
            ref_ids.append(ref_id)
        
        return ref_ids
    
    def _register_entity(self, entity_id: str, value: str, entity_type: str, attributes: Dict):
        """Register an entity if not already exists"""
        if entity_id not in self.entities_found:
            self.entities_found[entity_id] = Entity(
                id=entity_id,
                name=value,
                entity_type=entity_type,
                value=value,
                attributes=attributes
            )


# ============================================================================
# WORLD NETWORK BUILDER - Enhanced
# ============================================================================

class WorldNetworkBuilderV2:
    """
    Enhanced World Network Builder.
    Creates separate subgraphs for each claim type.
    """
    
    def __init__(self):
        self.node_counter = 0
        self.edge_counter = 0
    
    def _gen_node_id(self) -> str:
        self.node_counter += 1
        return f"node_{self.node_counter:04d}"
    
    def _gen_edge_id(self) -> str:
        self.edge_counter += 1
        return f"edge_{self.edge_counter:04d}"
    
    def build(self, parsed_data: Dict) -> WorldNetwork:
        """Build World Network from parsed SOP data"""
        network = WorldNetwork(
            document_id=parsed_data.get('document_id', 'unknown'),
            document_name=parsed_data.get('title', 'Untitled SOP')
        )
        
        # Add metadata
        network.metadata.update({
            'document_type': parsed_data.get('document_type', ''),
            'document_number': parsed_data.get('document_number', ''),
            'status': parsed_data.get('status', ''),
            'cause_explanation': parsed_data.get('cause_explanation', ''),
            'pend_code': parsed_data.get('pend_code', '')
        })
        
        # Add version history
        if parsed_data.get('revision_history'):
            for rev in parsed_data['revision_history']:
                version = Version(
                    revision=rev['revision'],
                    date=rev['date'],
                    description=rev['description'],
                    content_hash=hashlib.md5(rev['description'].encode()).hexdigest()[:8]
                )
                network.versions.append(version)
            network.current_version = parsed_data['revision_history'][0]['revision']
        
        # Create root node
        root_node = Node(
            id=self._gen_node_id(),
            node_type=NodeType.ROOT,
            content=parsed_data.get('title', 'SOP Root'),
            metadata={'document_number': parsed_data.get('document_number', '')}
        )
        network.add_node(root_node)
        
        # Build each claim type as a subgraph
        for claim_type_data in parsed_data.get('claim_types', []):
            self._build_claim_type_graph(network, claim_type_data, root_node.id)
        
        # Add entities
        for entity_id, entity_data in parsed_data.get('entities', {}).items():
            network.entities[entity_id] = Entity(**entity_data)
        
        # Add procedure references
        for ref_id, ref_data in parsed_data.get('procedure_refs', {}).items():
            network.procedure_refs[ref_id] = ProcedureReference(**ref_data)
        
        # Add lookup tables
        for table_name, entries in parsed_data.get('lookup_tables', {}).items():
            network.lookup_tables[table_name] = [ClinicEntry(**e) if isinstance(e, dict) else e for e in entries]
        
        return network
    
    def _build_claim_type_graph(self, network: WorldNetwork, claim_type_data: Dict, root_id: str):
        """Build a complete subgraph for a claim type"""
        
        # Create claim type root node
        claim_node = Node(
            id=self._gen_node_id(),
            node_type=NodeType.CLAIM_TYPE,
            content=claim_type_data['name'],
            parent_id=root_id,
            section=claim_type_data['name']
        )
        network.add_node(claim_node)
        
        # Connect to document root
        network.add_edge(Edge(
            id=self._gen_edge_id(),
            source_id=root_id,
            target_id=claim_node.id,
            edge_type=EdgeType.CONTAINS
        ))
        
        # Track terminal nodes for connecting "continue to next step"
        previous_step_terminals = []
        previous_step_id = claim_node.id
        
        # Build each step
        for step_data in claim_type_data.get('steps', []):
            step_id, terminals = self._build_step(
                network, step_data, claim_node.id, claim_type_data['name']
            )
            
            # Connect from claim type root to first step
            if previous_step_id == claim_node.id:
                network.add_edge(Edge(
                    id=self._gen_edge_id(),
                    source_id=claim_node.id,
                    target_id=step_id,
                    edge_type=EdgeType.CONTAINS
                ))
            else:
                # Connect "continue to next step" terminals to this step
                for term_id in previous_step_terminals:
                    term_node = network.nodes.get(term_id)
                    if term_node and ('continue' in term_node.content.lower() or 
                                     term_node.metadata.get('continue_to_step')):
                        network.add_edge(Edge(
                            id=self._gen_edge_id(),
                            source_id=term_id,
                            target_id=step_id,
                            edge_type=EdgeType.CONTINUE_TO_STEP,
                            condition="Continue"
                        ))
            
            previous_step_id = step_id
            previous_step_terminals = terminals
        
        return claim_node.id
    
    def _build_step(self, network: WorldNetwork, step_data: Dict, section_id: str, section_name: str) -> Tuple[str, List[str]]:
        """Build a step node and its branches"""
        
        question = step_data.get('question', f"Step {step_data.get('step_number', '?')}")
        is_decision = step_data.get('is_decision', False)
        
        # Create the step node
        step_node = Node(
            id=self._gen_node_id(),
            node_type=NodeType.DECISION if is_decision else NodeType.STEP,
            content=question,
            step_number=step_data.get('step_number'),
            parent_id=section_id,
            section=section_name,
            entities=step_data.get('entities', []),
            metadata={
                'notes': step_data.get('notes', []),
                'procedure_refs': step_data.get('procedure_refs', []),
                'is_decision': is_decision
            }
        )
        network.add_node(step_node)
        
        terminal_ids = []
        
        if is_decision and step_data.get('branches'):
            # Build branches
            for branch in step_data['branches']:
                branch_terminals = self._build_branch(network, branch, step_node.id, section_name)
                terminal_ids.extend(branch_terminals)
        else:
            # Non-decision step - it is itself a terminal
            terminal_ids.append(step_node.id)
        
        return step_node.id, terminal_ids
    
    def _build_branch(self, network: WorldNetwork, branch: Dict, parent_id: str, section_name: str) -> List[str]:
        """Build a branch node and return terminal node IDs"""
        terminal_ids = []
        
        branch_type = branch.get('type', 'unknown')
        action = branch.get('action', '')
        
        # Determine node and edge types
        if branch_type == 'yes':
            node_type = NodeType.BRANCH_YES
            edge_type = EdgeType.CONDITION_YES
        elif branch_type == 'no':
            node_type = NodeType.BRANCH_NO
            edge_type = EdgeType.CONDITION_NO
        else:
            node_type = NodeType.BRANCH_UNSURE
            edge_type = EdgeType.CONDITION_UNSURE
        
        # Create branch node
        branch_node = Node(
            id=self._gen_node_id(),
            node_type=node_type,
            content=action,
            parent_id=parent_id,
            section=section_name,
            entities=branch.get('entities', []),
            metadata={
                'procedure_refs': branch.get('procedure_refs', []),
                'continue_to_step': branch.get('continue_to_step'),
                'proceed_to_section': branch.get('proceed_to_section')
            }
        )
        network.add_node(branch_node)
        
        # Connect to parent decision
        network.add_edge(Edge(
            id=self._gen_edge_id(),
            source_id=parent_id,
            target_id=branch_node.id,
            edge_type=edge_type,
            condition=branch_type.upper(),
            condition_value=branch_type
        ))
        
        # Handle nested decisions
        if branch.get('nested_decisions'):
            for nested in branch['nested_decisions']:
                nested_type = nested.get('type', 'unknown')
                
                if nested_type == 'yes':
                    nested_node_type = NodeType.BRANCH_YES
                    nested_edge_type = EdgeType.NESTED_YES
                else:
                    nested_node_type = NodeType.BRANCH_NO
                    nested_edge_type = EdgeType.NESTED_NO
                
                nested_node = Node(
                    id=self._gen_node_id(),
                    node_type=nested_node_type,
                    content=nested.get('action', ''),
                    parent_id=branch_node.id,
                    section=section_name,
                    entities=nested.get('entities', []),
                    metadata={'procedure_refs': nested.get('procedure_refs', [])}
                )
                network.add_node(nested_node)
                
                network.add_edge(Edge(
                    id=self._gen_edge_id(),
                    source_id=branch_node.id,
                    target_id=nested_node.id,
                    edge_type=nested_edge_type,
                    condition=nested_type.upper()
                ))
                
                terminal_ids.append(nested_node.id)
        
        # Handle sub-scenarios
        if branch.get('sub_scenarios'):
            for scenario in branch['sub_scenarios']:
                scenario_node = Node(
                    id=self._gen_node_id(),
                    node_type=NodeType.ACTION,
                    content=scenario.get('action', ''),
                    parent_id=branch_node.id,
                    section=section_name,
                    entities=scenario.get('entities', []),
                    metadata={
                        'scenario_type': scenario.get('type', ''),
                        'procedure_refs': scenario.get('procedure_refs', [])
                    }
                )
                network.add_node(scenario_node)
                
                network.add_edge(Edge(
                    id=self._gen_edge_id(),
                    source_id=branch_node.id,
                    target_id=scenario_node.id,
                    edge_type=EdgeType.SEQUENCE,
                    condition=scenario.get('type', '')
                ))
                
                terminal_ids.append(scenario_node.id)
        
        # Add procedure reference nodes
        for ref_id in branch.get('procedure_refs', []):
            ref_node = Node(
                id=self._gen_node_id(),
                node_type=NodeType.REFERENCE,
                content=f"Refer to: {ref_id.replace('ref_', '').replace('_', '.')}",
                parent_id=branch_node.id,
                metadata={'reference_id': ref_id}
            )
            network.add_node(ref_node)
            
            network.add_edge(Edge(
                id=self._gen_edge_id(),
                source_id=branch_node.id,
                target_id=ref_node.id,
                edge_type=EdgeType.REFERENCE
            ))
        
        # If no nested/sub-scenarios, this branch is terminal
        if not branch.get('nested_decisions') and not branch.get('sub_scenarios'):
            terminal_ids.append(branch_node.id)
        
        return terminal_ids


# ============================================================================
# GRAPH VISUALIZER
# ============================================================================

class GraphVisualizerV2:
    """Generate visualizations for World Network"""
    
    @staticmethod
    def to_mermaid(network: WorldNetwork, claim_type: str = None, max_nodes: int = 100) -> str:
        """Generate Mermaid flowchart"""
        lines = ["flowchart TD"]
        lines.append("")
        
        # Styles
        lines.append("    %% Styles")
        lines.append("    classDef decision fill:#e3f2fd,stroke:#1565c0,stroke-width:2px")
        lines.append("    classDef yes fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px")
        lines.append("    classDef no fill:#ffebee,stroke:#c62828,stroke-width:2px")
        lines.append("    classDef unsure fill:#fff3e0,stroke:#ef6c00,stroke-width:2px")
        lines.append("    classDef section fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px")
        lines.append("    classDef action fill:#e8eaf6,stroke:#3f51b5")
        lines.append("    classDef reference fill:#fce4ec,stroke:#c2185b,stroke-dasharray: 5 5")
        lines.append("")
        
        # Filter nodes if specific claim type requested
        if claim_type and claim_type in network.claim_type_roots:
            nodes_to_include = set()
            root_id = network.claim_type_roots[claim_type]
            
            def collect_nodes(node_id):
                nodes_to_include.add(node_id)
                for edge in network.get_outgoing_edges(node_id):
                    if edge.target_id not in nodes_to_include:
                        collect_nodes(edge.target_id)
            
            collect_nodes(root_id)
            nodes = {k: v for k, v in network.nodes.items() if k in nodes_to_include}
            edges = {k: v for k, v in network.edges.items() 
                    if v.source_id in nodes_to_include and v.target_id in nodes_to_include}
        else:
            nodes = network.nodes
            edges = network.edges
        
        # Add nodes
        node_count = 0
        for node_id, node in nodes.items():
            if node_count >= max_nodes:
                break
            
            content = node.content[:60].replace('"', "'").replace('\n', ' ')
            content = re.sub(r'[^\w\s\-\.\,\?\!\:\;]', '', content)
            
            step_prefix = f"S{node.step_number}: " if node.step_number else ""
            
            if node.node_type == NodeType.DECISION:
                lines.append(f'    {node_id}{{"{step_prefix}{content}"}}')
                lines.append(f'    class {node_id} decision')
            elif node.node_type == NodeType.BRANCH_YES:
                lines.append(f'    {node_id}["{content}"]')
                lines.append(f'    class {node_id} yes')
            elif node.node_type == NodeType.BRANCH_NO:
                lines.append(f'    {node_id}["{content}"]')
                lines.append(f'    class {node_id} no')
            elif node.node_type == NodeType.BRANCH_UNSURE:
                lines.append(f'    {node_id}["{content}"]')
                lines.append(f'    class {node_id} unsure')
            elif node.node_type in [NodeType.CLAIM_TYPE, NodeType.ROOT]:
                lines.append(f'    {node_id}[["**{content}**"]]')
                lines.append(f'    class {node_id} section')
            elif node.node_type == NodeType.REFERENCE:
                lines.append(f'    {node_id}(("{content}"))')
                lines.append(f'    class {node_id} reference')
            elif node.node_type == NodeType.ACTION:
                lines.append(f'    {node_id}["{content}"]')
                lines.append(f'    class {node_id} action')
            else:
                lines.append(f'    {node_id}["{step_prefix}{content}"]')
            
            node_count += 1
        
        lines.append("")
        
        # Add edges
        for edge_id, edge in edges.items():
            if edge.source_id in nodes and edge.target_id in nodes:
                label = edge.condition if edge.condition else ""
                if label:
                    lines.append(f'    {edge.source_id} -->|{label}| {edge.target_id}')
                else:
                    lines.append(f'    {edge.source_id} --> {edge.target_id}')
        
        return '\n'.join(lines)
    
    @staticmethod
    def to_graphviz(network: WorldNetwork, claim_type: str = None) -> str:
        """Generate GraphViz DOT format"""
        lines = ["digraph WorldNetwork {"]
        lines.append('    rankdir=TB;')
        lines.append('    node [shape=box, style="rounded,filled", fontname="Arial"];')
        lines.append('    edge [fontname="Arial", fontsize=10];')
        lines.append('')
        
        # Color definitions
        colors = {
            NodeType.ROOT: '#e8f5e9',
            NodeType.CLAIM_TYPE: '#f3e5f5',
            NodeType.DECISION: '#e3f2fd',
            NodeType.BRANCH_YES: '#c8e6c9',
            NodeType.BRANCH_NO: '#ffcdd2',
            NodeType.BRANCH_UNSURE: '#ffe0b2',
            NodeType.ACTION: '#e8eaf6',
            NodeType.REFERENCE: '#fce4ec',
            NodeType.STEP: '#f5f5f5'
        }
        
        # Filter by claim type if specified
        if claim_type:
            subgraph = network.get_claim_type_graph(claim_type)
            nodes = {k: network.nodes[k] for k in subgraph.get('nodes', {}).keys()}
            edges = {k: network.edges[k] for k in subgraph.get('edges', {}).keys()}
        else:
            nodes = network.nodes
            edges = network.edges
        
        for node_id, node in nodes.items():
            content = node.content[:50].replace('"', '\\"').replace('\n', '\\n')
            color = colors.get(node.node_type, '#ffffff')
            
            shape = 'box'
            if node.node_type == NodeType.DECISION:
                shape = 'diamond'
            elif node.node_type == NodeType.ROOT:
                shape = 'house'
            elif node.node_type == NodeType.CLAIM_TYPE:
                shape = 'folder'
            elif node.node_type == NodeType.REFERENCE:
                shape = 'ellipse'
            
            label = content
            if node.step_number:
                label = f"Step {node.step_number}:\\n{content}"
            
            lines.append(f'    "{node_id}" [label="{label}", shape={shape}, fillcolor="{color}"];')
        
        lines.append('')
        
        for edge_id, edge in edges.items():
            label_part = f' [label="{edge.condition}"]' if edge.condition else ''
            lines.append(f'    "{edge.source_id}" -> "{edge.target_id}"{label_part};')
        
        lines.append('}')
        return '\n'.join(lines)
    
    @staticmethod
    def to_html_interactive(network: WorldNetwork) -> str:
        """Generate interactive HTML tree visualization using SVG (works in artifact viewer)"""
        
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
    <title>World Network Tree - {network.document_name}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f8f9fa;
            overflow: hidden;
        }}
        .header {{
            background: white;
            padding: 12px 20px;
            border-bottom: 1px solid #e0e0e0;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 100;
        }}
        .header h1 {{ font-size: 16px; color: #333; margin-bottom: 8px; }}
        .header-info {{ font-size: 12px; color: #666; margin-bottom: 8px; }}
        .claim-buttons {{ display: flex; gap: 6px; flex-wrap: wrap; }}
        .claim-btn {{
            padding: 5px 10px;
            border: 1px solid #ddd;
            background: white;
            border-radius: 14px;
            cursor: pointer;
            font-size: 11px;
            transition: all 0.2s;
        }}
        .claim-btn:hover {{ background: #f0f0f0; }}
        .claim-btn.active {{ background: #2196f3; color: white; border-color: #2196f3; }}
        
        .legend {{
            position: fixed;
            top: 100px;
            right: 15px;
            background: white;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            z-index: 100;
            font-size: 11px;
        }}
        .legend-title {{ font-weight: 600; margin-bottom: 6px; }}
        .legend-item {{ display: flex; align-items: center; gap: 6px; margin: 3px 0; }}
        .legend-diamond {{ width: 10px; height: 10px; background: #2196f3; transform: rotate(45deg); border-radius: 2px; }}
        .legend-rect {{ width: 12px; height: 8px; border-radius: 2px; }}
        
        .svg-container {{
            margin-top: 95px;
            overflow: auto;
            height: calc(100vh - 95px);
            cursor: grab;
        }}
        .svg-container:active {{ cursor: grabbing; }}
        
        #treeSvg {{
            display: block;
            min-width: 100%;
            min-height: 100%;
        }}
        
        .tooltip {{
            position: fixed;
            background: rgba(0,0,0,0.85);
            color: white;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 11px;
            max-width: 280px;
            pointer-events: none;
            z-index: 1000;
            display: none;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            line-height: 1.4;
        }}
        .tooltip strong {{ color: #4fc3f7; }}
        
        .controls {{
            position: fixed;
            bottom: 15px;
            left: 15px;
            display: flex;
            gap: 6px;
            z-index: 100;
        }}
        .control-btn {{
            width: 32px;
            height: 32px;
            border: none;
            background: white;
            border-radius: 50%;
            cursor: pointer;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            font-size: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        .control-btn:hover {{ background: #f0f0f0; }}
        
        .tree-node {{ cursor: pointer; }}
        .tree-node:hover rect, .tree-node:hover .diamond {{ filter: brightness(1.1); }}
        .tree-edge {{ fill: none; stroke: #999; stroke-width: 1.5; }}
        .edge-label {{ font-size: 9px; fill: #666; }}
        .node-label {{ font-size: 10px; fill: #333; text-anchor: middle; }}
        .node-label-white {{ font-size: 10px; fill: white; text-anchor: middle; dominant-baseline: middle; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🌳 {network.document_name}</h1>
        <div class="header-info">
            Document: {network.document_id} | Version: {network.current_version} | 
            Nodes: {len(network.nodes)} | Edges: {len(network.edges)}
        </div>
        <div class="claim-buttons">
            {claim_buttons}
        </div>
    </div>
    
    <div class="legend">
        <div class="legend-title">Legend</div>
        <div class="legend-item"><div class="legend-diamond"></div> Decision</div>
        <div class="legend-item"><div class="legend-rect" style="background:#4caf50"></div> Yes Branch</div>
        <div class="legend-item"><div class="legend-rect" style="background:#f44336"></div> No Branch</div>
        <div class="legend-item"><div class="legend-rect" style="background:#ff9800"></div> Unsure</div>
        <div class="legend-item"><div class="legend-rect" style="background:#9c27b0"></div> Claim Type</div>
        <div class="legend-item"><div class="legend-rect" style="background:#607d8b"></div> Action/Step</div>
    </div>
    
    <div class="controls">
        <button class="control-btn" onclick="zoomIn()" title="Zoom In">+</button>
        <button class="control-btn" onclick="zoomOut()" title="Zoom Out">−</button>
        <button class="control-btn" onclick="resetView()" title="Reset">↺</button>
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
        
        let scale = 1;
        
        const NODE_WIDTH = 130;
        const NODE_HEIGHT = 32;
        const LEVEL_HEIGHT = 75;
        const NODE_SPACING = 15;
        const DIAMOND_SIZE = 22;
        
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
            if (!node) return {{ width: 0, nodes: [], edges: [] }};
            
            const children = node.children || [];
            let nodes = [];
            let edges = [];
            
            if (children.length === 0) {{
                const x = leftOffset + NODE_WIDTH / 2;
                const y = level * LEVEL_HEIGHT + NODE_HEIGHT / 2 + 20;
                nodes.push({{ ...node, x, y }});
                return {{ width: NODE_WIDTH + NODE_SPACING, nodes, edges }};
            }}
            
            let currentOffset = leftOffset;
            let childResults = [];
            
            for (const child of children) {{
                const result = calculateTreeLayout(child, level + 1, currentOffset);
                childResults.push(result);
                nodes = nodes.concat(result.nodes);
                edges = edges.concat(result.edges);
                currentOffset += result.width;
            }}
            
            const totalWidth = childResults.reduce((sum, r) => sum + r.width, 0);
            const firstChildX = childResults[0].nodes[0]?.x || leftOffset;
            const lastChildX = childResults[childResults.length - 1].nodes[0]?.x || leftOffset;
            const centerX = (firstChildX + lastChildX) / 2;
            const y = level * LEVEL_HEIGHT + NODE_HEIGHT / 2 + 20;
            
            nodes.unshift({{ ...node, x: centerX, y }});
            
            // Create edges to children
            for (const child of children) {{
                const childNode = nodes.find(n => n.id === child.id);
                if (childNode) {{
                    edges.push({{
                        fromX: centerX,
                        fromY: y,
                        toX: childNode.x,
                        toY: childNode.y,
                        label: child.edgeLabel || ''
                    }});
                }}
            }}
            
            return {{ width: Math.max(totalWidth, NODE_WIDTH + NODE_SPACING), nodes, edges }};
        }}
        
        function renderTree() {{
            const tree = treesData[currentClaimType];
            if (!tree) {{
                svg.innerHTML = '<text x="50" y="50" fill="#666">No data for this claim type</text>';
                return;
            }}
            
            const result = calculateTreeLayout(tree, 0, 50);
            const {{ nodes, edges }} = result;
            
            // Calculate SVG size
            let maxX = 0, maxY = 0;
            nodes.forEach(n => {{
                maxX = Math.max(maxX, n.x + NODE_WIDTH);
                maxY = Math.max(maxY, n.y + NODE_HEIGHT + 30);
            }});
            
            const width = Math.max(maxX + 100, 800);
            const height = Math.max(maxY + 50, 600);
            
            svg.setAttribute('width', width * scale);
            svg.setAttribute('height', height * scale);
            svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`);
            
            let svgContent = '';
            
            // Draw edges first
            edges.forEach(edge => {{
                const midY = (edge.fromY + edge.toY) / 2;
                const fromYOffset = edge.fromY + (nodes.find(n => n.x === edge.fromX && n.y === edge.fromY)?.type?.includes('decision') ? DIAMOND_SIZE/2 + 5 : NODE_HEIGHT/2);
                const toYOffset = edge.toY - NODE_HEIGHT/2;
                
                svgContent += `<path class="tree-edge" d="M ${{edge.fromX}} ${{fromYOffset}} C ${{edge.fromX}} ${{midY}}, ${{edge.toX}} ${{midY}}, ${{edge.toX}} ${{toYOffset}}"/>`;
                
                if (edge.label) {{
                    const labelX = (edge.fromX + edge.toX) / 2;
                    const labelY = midY - 3;
                    svgContent += `<text class="edge-label" x="${{labelX}}" y="${{labelY}}" text-anchor="middle">${{edge.label}}</text>`;
                }}
            }});
            
            // Draw nodes
            nodes.forEach(node => {{
                const color = colors[node.type] || '#999';
                const isDecision = node.type === 'decision' || node.type === 'sub_decision';
                const escapedContent = (node.fullContent || node.name || '').replace(/"/g, '&quot;');
                
                if (isDecision) {{
                    // Diamond shape
                    const size = DIAMOND_SIZE;
                    svgContent += `<g class="tree-node" onmouseover="showTooltip(event, '${{node.type}}', '${{escapedContent}}')" onmouseout="hideTooltip()">`;
                    svgContent += `<rect class="diamond" x="${{node.x - size/2}}" y="${{node.y - size/2}}" width="${{size}}" height="${{size}}" fill="${{color}}" stroke="white" stroke-width="2" rx="3" transform="rotate(45 ${{node.x}} ${{node.y}})"/>`;
                    const label = node.name.length > 22 ? node.name.substring(0, 22) + '...' : node.name;
                    svgContent += `<text class="node-label" x="${{node.x}}" y="${{node.y + size/2 + 14}}">${{label}}</text>`;
                    svgContent += '</g>';
                }} else {{
                    // Rounded rectangle
                    svgContent += `<g class="tree-node" onmouseover="showTooltip(event, '${{node.type}}', '${{escapedContent}}')" onmouseout="hideTooltip()">`;
                    svgContent += `<rect x="${{node.x - NODE_WIDTH/2}}" y="${{node.y - NODE_HEIGHT/2}}" width="${{NODE_WIDTH}}" height="${{NODE_HEIGHT}}" fill="${{color}}" stroke="white" stroke-width="2" rx="6"/>`;
                    const label = node.name.length > 18 ? node.name.substring(0, 18) + '...' : node.name;
                    svgContent += `<text class="node-label-white" x="${{node.x}}" y="${{node.y}}">${{label}}</text>`;
                    svgContent += '</g>';
                }}
            }});
            
            svg.innerHTML = svgContent;
        }}
        
        function showTooltip(event, type, content) {{
            tooltip.innerHTML = '<strong>' + type.replace('_', ' ') + '</strong><br>' + content;
            tooltip.style.display = 'block';
            tooltip.style.left = (event.clientX + 15) + 'px';
            tooltip.style.top = (event.clientY + 15) + 'px';
        }}
        
        function hideTooltip() {{
            tooltip.style.display = 'none';
        }}
        
        function showClaimType(claimType) {{
            currentClaimType = claimType;
            document.querySelectorAll('.claim-btn').forEach(btn => {{
                btn.classList.toggle('active', btn.textContent === claimType);
            }});
            renderTree();
        }}
        
        function zoomIn() {{ scale = Math.min(scale * 1.2, 3); renderTree(); }}
        function zoomOut() {{ scale = Math.max(scale / 1.2, 0.3); renderTree(); }}
        function resetView() {{ scale = 1; svgContainer.scrollTo(0, 0); renderTree(); }}
        
        // Initial render
        renderTree();
    </script>
</body>
</html>'''
        return html


# ============================================================================
# DECISION TREE FORMATTER
# ============================================================================

class DecisionTreeFormatter:
    """Format World Network as readable decision tree"""
    
    @staticmethod
    def format(network: WorldNetwork) -> str:
        """Format as human-readable decision tree"""
        lines = []
        lines.append("=" * 80)
        lines.append(f"WORLD NETWORK - DETERMINISTIC DECISION TREE")
        lines.append(f"Document: {network.document_name} ({network.document_id})")
        lines.append(f"Version: {network.current_version}")
        lines.append("=" * 80)
        
        # Version history
        if network.versions:
            lines.append("\nREVISION HISTORY:")
            lines.append("-" * 40)
            for v in network.versions:
                lines.append(f"  v{v.revision} ({v.date})")
                lines.append(f"    {v.description[:100]}")
        
        # Format each claim type
        for claim_type, root_id in network.claim_type_roots.items():
            lines.append("\n" + "=" * 80)
            lines.append(f"CLAIM TYPE: {claim_type}")
            lines.append("=" * 80)
            
            # Get all nodes in this claim type
            lines.extend(DecisionTreeFormatter._format_subtree(network, root_id, 0))
        
        # Entities summary
        lines.append("\n" + "=" * 80)
        lines.append("OBSERVATION NETWORK - ENTITIES")
        lines.append("=" * 80)
        
        entities_by_type = defaultdict(list)
        for eid, entity in network.entities.items():
            entities_by_type[entity.entity_type].append(entity.name)
        
        for etype, names in sorted(entities_by_type.items()):
            lines.append(f"\n{etype.upper()}:")
            for name in sorted(set(names)):
                lines.append(f"  • {name}")
        
        # Procedure references
        if network.procedure_refs:
            lines.append("\n" + "=" * 80)
            lines.append("DEEP LINK REFERENCES")
            lines.append("=" * 80)
            for ref_id, ref in network.procedure_refs.items():
                lines.append(f"  • {ref.procedure_code}")
                if ref.procedure_name:
                    lines.append(f"    Name: {ref.procedure_name}")
                lines.append(f"    Status: {'Resolved' if ref.resolved else 'Pending resolution'}")
        
        return '\n'.join(lines)
    
    @staticmethod
    def _format_subtree(network: WorldNetwork, node_id: str, depth: int) -> List[str]:
        """Recursively format a subtree"""
        lines = []
        indent = "  " * depth
        
        node = network.nodes.get(node_id)
        if not node:
            return lines
        
        # Format based on node type
        if node.node_type == NodeType.DECISION:
            step_label = f"[STEP {node.step_number}]" if node.step_number else "[DECISION]"
            lines.append(f"\n{indent}{step_label}")
            lines.append(f"{indent}  QUESTION: {node.content[:150]}")
        elif node.node_type == NodeType.STEP:
            step_label = f"[STEP {node.step_number}]" if node.step_number else "[STEP]"
            lines.append(f"\n{indent}{step_label}")
            lines.append(f"{indent}  ACTION: {node.content[:150]}")
        elif node.node_type == NodeType.BRANCH_YES:
            lines.append(f"{indent}  └─ YES: {node.content[:100]}")
        elif node.node_type == NodeType.BRANCH_NO:
            lines.append(f"{indent}  └─ NO: {node.content[:100]}")
        elif node.node_type == NodeType.BRANCH_UNSURE:
            lines.append(f"{indent}  └─ UNSURE: {node.content[:100]}")
        elif node.node_type == NodeType.REFERENCE:
            lines.append(f"{indent}      📎 {node.content}")
        elif node.node_type == NodeType.ACTION:
            scenario = node.metadata.get('scenario_type', '')
            prefix = f"[{scenario}] " if scenario else ""
            lines.append(f"{indent}    → {prefix}{node.content[:80]}")
        
        # Process children
        for edge in network.get_outgoing_edges(node_id):
            if edge.edge_type != EdgeType.REFERENCE:  # References handled inline
                lines.extend(DecisionTreeFormatter._format_subtree(network, edge.target_id, depth + 1))
        
        return lines


# ============================================================================
# MAIN PROCESSOR
# ============================================================================

class SOPToWorldNetworkProcessorV2:
    """Main orchestrator for Phase 1 SOP processing"""
    
    def __init__(self):
        self.parser = SOPParserV2()
        self.builder = WorldNetworkBuilderV2()
        self.observation_network = ObservationNetwork()
    
    def process(self, markdown_content: str, document_id: str = None) -> Dict:
        """Process SOP content and generate all outputs"""
        
        # Parse SOP
        print("Parsing SOP...")
        parsed_data = self.parser.parse(markdown_content, document_id)
        
        # Build World Network
        print("Building World Network...")
        world_network = self.builder.build(parsed_data)
        
        # Update Observation Network
        print("Extracting Observation Network...")
        self.observation_network.absorb_from_world_network(world_network)
        
        # Generate visualizations
        print("Generating visualizations...")
        mermaid_all = GraphVisualizerV2.to_mermaid(world_network)
        graphviz_all = GraphVisualizerV2.to_graphviz(world_network)
        html_interactive = GraphVisualizerV2.to_html_interactive(world_network)
        
        # Generate per-claim-type visualizations
        claim_type_mermaid = {}
        claim_type_graphviz = {}
        for claim_type in world_network.claim_type_roots.keys():
            claim_type_mermaid[claim_type] = GraphVisualizerV2.to_mermaid(world_network, claim_type)
            claim_type_graphviz[claim_type] = GraphVisualizerV2.to_graphviz(world_network, claim_type)
        
        # Format decision tree
        decision_tree = DecisionTreeFormatter.format(world_network)
        
        # Compute statistics
        stats = self._compute_statistics(world_network)
        
        return {
            'parsed_data': parsed_data,
            'world_network': world_network,
            'observation_network': self.observation_network,
            'visualizations': {
                'mermaid': mermaid_all,
                'graphviz': graphviz_all,
                'html': html_interactive,
                'by_claim_type': {
                    'mermaid': claim_type_mermaid,
                    'graphviz': claim_type_graphviz
                }
            },
            'decision_tree': decision_tree,
            'statistics': stats
        }
    
    def _compute_statistics(self, network: WorldNetwork) -> Dict:
        """Compute network statistics"""
        node_types = defaultdict(int)
        for node in network.nodes.values():
            node_types[node.node_type.value] += 1
        
        edge_types = defaultdict(int)
        for edge in network.edges.values():
            edge_types[edge.edge_type.value] += 1
        
        # Depth calculation per claim type
        max_depths = {}
        for claim_type, root_id in network.claim_type_roots.items():
            max_depth = 0
            visited = set()
            
            def calc_depth(node_id, depth):
                nonlocal max_depth
                if node_id in visited:
                    return
                visited.add(node_id)
                max_depth = max(max_depth, depth)
                for edge in network.get_outgoing_edges(node_id):
                    calc_depth(edge.target_id, depth + 1)
            
            calc_depth(root_id, 0)
            max_depths[claim_type] = max_depth
        
        return {
            'total_nodes': len(network.nodes),
            'total_edges': len(network.edges),
            'node_types': dict(node_types),
            'edge_types': dict(edge_types),
            'claim_types': list(network.claim_type_roots.keys()),
            'num_claim_types': len(network.claim_type_roots),
            'decision_points': node_types.get('decision', 0),
            'procedure_references': len(network.procedure_refs),
            'unique_entities': len(network.entities),
            'versions': len(network.versions),
            'current_version': network.current_version,
            'max_depths': max_depths,
            'lookup_tables': {k: len(v) for k, v in network.lookup_tables.items()}
        }


# ============================================================================
# CLI ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import sys
    
    print("=" * 80)
    print("PHASE 1: WORLD NETWORK BUILDER v2.0")
    print("=" * 80)
    
    # Example usage with sample content
    sample = '''# **BC - Determine If BlueCard Claim**

**Document Type:** SMA **Document Number:** P966

**Status:** Approved and Released

### **Amazon Claims**

1. Is the provider Vita Health?

   - **Yes:** Assign Provider ID **B09B4VB09B4V** and process directly.
   - **No:** Continue to the next step.

2. Is the provider Concentra with TIN 752510547?

   - **Yes:** Follow scenario below:
     - **Provider-submitted:** Handle direct using **B02KFVB02KFV**.
     - **Member-submitted:** Send back. Refer to PR.OP.CL.2862.
   - **No:** Continue to the next step.
'''
    
    processor = SOPToWorldNetworkProcessorV2()
    result = processor.process(sample, "P966")
    
    print("\n=== Statistics ===")
    print(json.dumps(result['statistics'], indent=2))
    
    print("\n=== Decision Tree Preview ===")
    print(result['decision_tree'][:2000])
