#!/usr/bin/env python3
"""
World Network Builder - Complete Script with Deep Linking
Supports clicking reference nodes to expand child procedure trees
"""
import re, json, hashlib, sys, os, glob
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from enum import Enum
from html.parser import HTMLParser

class NodeType(Enum):
    ROOT = "root"
    CLAIM_TYPE = "claim_type"
    DECISION = "decision"
    ACTION = "action"
    BRANCH_YES = "branch_yes"
    BRANCH_NO = "branch_no"
    BRANCH_UNSURE = "branch_unsure"
    SUB_CONDITION = "sub_condition"
    REFERENCE = "reference"
    LINKED_PROCEDURE = "linked_procedure"
    STEP = "step"

class EdgeType(Enum):
    SEQUENCE = "sequence"
    CONDITION_YES = "condition_yes"
    CONDITION_NO = "condition_no"
    CONDITION_UNSURE = "condition_unsure"
    SUB_YES = "sub_yes"
    SUB_NO = "sub_no"
    REFERENCE = "reference"
    DEEP_LINK = "deep_link"
    CONTAINS = "contains"
    SUB_CONDITION = "sub_condition"

class LinkStatus(Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    NOT_FOUND = "not_found"
    ERROR = "error"

@dataclass
class Entity:
    id: str; name: str; entity_type: str; value: str = ""
    def to_dict(self): return asdict(self)

@dataclass
class NetworkNode:
    id: str; node_type: NodeType; content: str; section: Optional[str] = None
    step_number: Optional[int] = None; metadata: Dict[str, Any] = field(default_factory=dict)
    procedure_code: Optional[str] = None
    def to_dict(self):
        d = asdict(self); d['node_type'] = self.node_type.value; return d

@dataclass
class NetworkEdge:
    id: str; source_id: str; target_id: str; edge_type: EdgeType; condition: Optional[str] = None
    def to_dict(self):
        d = asdict(self); d['edge_type'] = self.edge_type.value; return d

@dataclass
class ProcedureReference:
    id: str; procedure_code: str; title: str = ""; status: str = "pending"
    source_file: Optional[str] = None; error_message: Optional[str] = None
    def to_dict(self): return asdict(self)

@dataclass
class VersionInfo:
    revision: str; date: str; description: str = ""
    def to_dict(self): return asdict(self)

class WorldNetwork:
    def __init__(self, doc_id: str, doc_name: str):
        self.document_id = doc_id
        self.document_name = doc_name
        self.current_version = "1.0"
        self.nodes: Dict[str, NetworkNode] = {}
        self.edges: Dict[str, NetworkEdge] = {}
        self.entities: Dict[str, Entity] = {}
        self.procedure_refs: Dict[str, ProcedureReference] = {}
        self.versions: List[VersionInfo] = []
        self.claim_type_roots: Dict[str, str] = {}
        self.linked_procedures: Dict[str, str] = {}
        self.metadata: Dict[str, Any] = {}
        self._nc = 0; self._ec = 0

    def create_node(self, nt: NodeType, content: str, **kw) -> NetworkNode:
        self._nc += 1; nid = f"node_{self._nc:04d}"
        n = NetworkNode(id=nid, node_type=nt, content=content, **kw)
        self.nodes[nid] = n; return n

    def create_edge(self, src: str, tgt: str, et: EdgeType, cond: Optional[str] = None) -> NetworkEdge:
        self._ec += 1; eid = f"edge_{self._ec:04d}"
        e = NetworkEdge(id=eid, source_id=src, target_id=tgt, edge_type=et, condition=cond)
        self.edges[eid] = e; return e

    def get_outgoing_edges(self, nid: str) -> List[NetworkEdge]:
        return [e for e in self.edges.values() if e.source_id == nid]

    def to_dict(self) -> Dict:
        return {
            'document_id': self.document_id, 'document_name': self.document_name,
            'current_version': self.current_version,
            'nodes': {k: v.to_dict() for k, v in self.nodes.items()},
            'edges': {k: v.to_dict() for k, v in self.edges.items()},
            'entities': {k: v.to_dict() for k, v in self.entities.items()},
            'procedure_refs': {k: v.to_dict() for k, v in self.procedure_refs.items()},
            'versions': [v.to_dict() for v in self.versions],
            'claim_type_roots': self.claim_type_roots,
            'linked_procedures': self.linked_procedures, 'metadata': self.metadata
        }


class SOPParser:
    PATTERNS = [
        r'###\s*\*\*\s*(Amazon\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Alaska\s+Air.*?Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Microsoft\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(Expedia\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(FEP\s+Claims?)\s*\*\*',
        r'###\s*\*\*\s*(LEOFF.*?)\s*\*\*',
        r'###\s*\*\*\s*(All\s+Others?)\s*\*\*',
        r'^#+\s*(Amazon\s+Claims?)',
        r'^#+\s*(Microsoft\s+Claims?)',
        r'^#+\s*(All\s+Others?)',
        r'^###\s*\*\*([^*]+)\*\*',
    ]
    
    STEP_PAT = re.compile(r'^(\d+)\.\s+(.+)', re.MULTILINE)
    DEC_PAT = re.compile(r'^(?:Is|Does|Did|Are|Has|Have|Was|Were|Can|Should|Will|Would)\s+', re.IGNORECASE)
    PROC_PAT = re.compile(r'(PR\.OP\.CL\.\d{4})')
    VER_PAT = re.compile(r'^\|\s*(\d+\.\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{4}[^|]*)\s*\|\s*([^|]+)\s*\|', re.MULTILINE)
    YES_PAT = re.compile(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Yes)\s*[:\*\*]*\s*(.*)', re.IGNORECASE)
    NO_PAT = re.compile(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(No)\s*[:\*\*]*\s*(.*)', re.IGNORECASE)
    UNSURE_PAT = re.compile(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Unsure)\s*[:\*\*]*\s*(.*)', re.IGNORECASE)
    SUB_COND_PAT = re.compile(r'^\s*[-*]?\s*\*?\*?([A-Z][a-z]+(?:-[a-z]+)?(?:\s+[a-z]+)?)\s*[:\*\*]+\s*(.*)', re.IGNORECASE)
    
    def parse(self, text): return {'document_info': self._doc_info(text), 'versions': self._versions(text), 'sections': self._sections(text), 'procedure_references': self._all_refs(text), 'raw_text': text}
    def _doc_info(self, t):
        info = {'title': '', 'document_id': '', 'status': ''}
        m = re.search(r'^#\s+\*?\*?(.+?)\*?\*?\s*$', t, re.MULTILINE)
        if m: info['title'] = m.group(1).strip()
        m = re.search(r'\b(P\d{3,4})\b', t)
        if m: info['document_id'] = m.group(1)
        if 'CURRENT' in t.upper() or 'Approved' in t: info['status'] = 'Current'
        return info
    def _versions(self, t): return [{'revision': m.group(1), 'date': m.group(2).strip(), 'description': m.group(3).strip()} for m in self.VER_PAT.finditer(t)]
    def _sections(self, t):
        matches = []; seen = set()
        for p in self.PATTERNS:
            for m in re.finditer(p, t, re.MULTILINE | re.IGNORECASE):
                n = m.group(1).strip().lower()
                if n not in seen and len(n) > 3: seen.add(n); matches.append((m.start(), m.group(1).strip()))
        matches.sort(key=lambda x: x[0]); secs = []
        for i, (pos, name) in enumerate(matches):
            end = matches[i + 1][0] if i + 1 < len(matches) else len(t); txt = t[pos:end]
            secs.append({'name': name, 'steps': self._steps(txt), 'procedure_refs': list(set(self.PROC_PAT.findall(txt)))})
        return secs
    def _steps(self, t):
        steps = []; ms = list(self.STEP_PAT.finditer(t))
        for i, m in enumerate(ms):
            end = ms[i + 1].start() if i + 1 < len(ms) else len(t); txt = t[m.start():end].strip(); content = m.group(2).strip()
            is_dec = bool(self.DEC_PAT.search(content)) or '?' in content
            steps.append({'number': int(m.group(1)), 'content': content, 'full_text': txt, 'is_decision': is_dec, 'branches': self._parse_branches(txt) if is_dec else [], 'procedure_refs': list(set(self.PROC_PAT.findall(txt)))})
        return steps
    def _parse_branches(self, step_text):
        branches = []; lines = step_text.split('\n'); current_branch = None; current_sub = None; branch_indent = 0
        for line in lines:
            stripped = line.strip()
            if not stripped: continue
            leading = len(line) - len(line.lstrip())
            yes_m = self.YES_PAT.match(stripped); no_m = self.NO_PAT.match(stripped); unsure_m = self.UNSURE_PAT.match(stripped)
            if yes_m:
                if current_branch:
                    if current_sub: current_branch['sub_conditions'].append(current_sub); current_sub = None
                    branches.append(current_branch)
                current_branch = {'type': 'yes', 'content': yes_m.group(2).strip(), 'sub_conditions': [], 'procedure_refs': [], 'indent': leading}; branch_indent = leading
            elif no_m:
                if current_branch:
                    if current_sub: current_branch['sub_conditions'].append(current_sub); current_sub = None
                    branches.append(current_branch)
                current_branch = {'type': 'no', 'content': no_m.group(2).strip(), 'sub_conditions': [], 'procedure_refs': [], 'indent': leading}; branch_indent = leading
            elif unsure_m:
                if current_branch:
                    if current_sub: current_branch['sub_conditions'].append(current_sub); current_sub = None
                    branches.append(current_branch)
                current_branch = {'type': 'unsure', 'content': unsure_m.group(2).strip(), 'sub_conditions': [], 'procedure_refs': [], 'indent': leading}; branch_indent = leading
            elif current_branch:
                nested_yes = re.match(r'^\s*I?\s*\*?\*?(Yes)\s*[:\*\*]+\s*(.*)', stripped, re.IGNORECASE)
                nested_no = re.match(r'^\s*I?\s*\*?\*?(No)\s*[:\*\*]+\s*(.*)', stripped, re.IGNORECASE)
                sub_m = self.SUB_COND_PAT.match(stripped)
                if nested_yes and leading > branch_indent:
                    if current_sub: current_branch['sub_conditions'].append(current_sub)
                    current_sub = {'type': 'yes', 'label': 'Yes', 'content': nested_yes.group(2).strip(), 'procedure_refs': list(set(self.PROC_PAT.findall(nested_yes.group(2))))}
                elif nested_no and leading > branch_indent:
                    if current_sub: current_branch['sub_conditions'].append(current_sub)
                    current_sub = {'type': 'no', 'label': 'No', 'content': nested_no.group(2).strip(), 'procedure_refs': list(set(self.PROC_PAT.findall(nested_no.group(2))))}
                elif sub_m and leading > branch_indent:
                    label = sub_m.group(1).strip()
                    if label.lower() not in ['important', 'note', 'page', 'refer', 'the', 'when', 'using', 'location']:
                        if current_sub: current_branch['sub_conditions'].append(current_sub)
                        current_sub = {'type': 'sub', 'label': label, 'content': sub_m.group(2).strip(), 'procedure_refs': list(set(self.PROC_PAT.findall(sub_m.group(2))))}
                    elif current_sub: current_sub['content'] += ' ' + stripped
                elif current_sub: current_sub['content'] += ' ' + stripped; current_sub['procedure_refs'] = list(set(self.PROC_PAT.findall(current_sub['content'])))
                else: current_branch['content'] += ' ' + stripped
        if current_branch:
            if current_sub: current_branch['sub_conditions'].append(current_sub)
            branches.append(current_branch)
        for b in branches: b['procedure_refs'] = list(set(self.PROC_PAT.findall(b['content'])))
        return branches
    def _all_refs(self, t):
        seen = set(); refs = []
        for m in self.PROC_PAT.finditer(t):
            c = m.group(1)
            if c in seen: continue
            seen.add(c); tm = re.search(rf'{c}\s*[-:]\s*([^.\n]+)', t)
            refs.append({'code': c, 'title': tm.group(1).strip() if tm else ''})
        return refs


class WorldNetworkBuilder:
    def __init__(self): self.network = None
    def build(self, parsed, doc_id, doc_name):
        self.network = WorldNetwork(doc_id, doc_name)
        info = parsed.get('document_info', {}); self.network.metadata = {'title': info.get('title', ''), 'status': info.get('status', '')}
        for v in parsed.get('versions', []): self.network.versions.append(VersionInfo(revision=v.get('revision', ''), date=v.get('date', ''), description=v.get('description', '')))
        if self.network.versions: self.network.current_version = self.network.versions[0].revision
        root = self.network.create_node(NodeType.ROOT, doc_name)
        for sec in parsed.get('sections', []): self._proc_section(sec, root.id)
        for ref in parsed.get('procedure_references', []):
            c = ref['code']
            if c not in self.network.procedure_refs: self.network.procedure_refs[c] = ProcedureReference(id=f"ref_{c}", procedure_code=c, title=ref.get('title', ''), status=LinkStatus.PENDING.value)
        self._extract_entities(parsed); return self.network
    def _proc_section(self, sec, pid):
        name = sec['name']; ct = self.network.create_node(NodeType.CLAIM_TYPE, name, section=name)
        self.network.create_edge(pid, ct.id, EdgeType.CONTAINS); self.network.claim_type_roots[name] = ct.id; prev = ct.id
        for step in sec.get('steps', []): prev = self._proc_step(step, ct.id, prev, name)
    def _proc_step(self, step, sid, prev, sec):
        num, content, is_dec = step['number'], step['content'], step['is_decision']
        if is_dec:
            dn = self.network.create_node(NodeType.DECISION, content, section=sec, step_number=num)
            self.network.create_edge(prev, dn.id, EdgeType.SEQUENCE)
            for b in step.get('branches', []): self._proc_branch(b, dn.id, sec)
            return dn.id
        else:
            sn = self.network.create_node(NodeType.STEP, content, section=sec, step_number=num)
            self.network.create_edge(prev, sn.id, EdgeType.SEQUENCE)
            for pc in step.get('procedure_refs', []): self._add_ref(pc, sn.id)
            return sn.id
    def _proc_branch(self, b, pid, sec):
        bt = b['type'].lower(); content = b['content']
        if bt == 'yes': nt, et, cond = NodeType.BRANCH_YES, EdgeType.CONDITION_YES, "YES"
        elif bt == 'no': nt, et, cond = NodeType.BRANCH_NO, EdgeType.CONDITION_NO, "NO"
        else: nt, et, cond = NodeType.BRANCH_UNSURE, EdgeType.CONDITION_UNSURE, "UNSURE"
        display = content[:100] + '...' if len(content) > 100 else content
        bn = self.network.create_node(nt, display, section=sec)
        self.network.create_edge(pid, bn.id, et, cond)
        for pc in b.get('procedure_refs', []): self._add_ref(pc, bn.id)
        for sub in b.get('sub_conditions', []): self._proc_sub(sub, bn.id, sec)
        return bn.id
    def _proc_sub(self, sub, pid, sec):
        st, label, content = sub.get('type', 'sub'), sub.get('label', ''), sub.get('content', '')
        if st == 'yes': nt, et, cond = NodeType.BRANCH_YES, EdgeType.SUB_YES, "YES"
        elif st == 'no': nt, et, cond = NodeType.BRANCH_NO, EdgeType.SUB_NO, "NO"
        else: nt, et, cond = NodeType.SUB_CONDITION, EdgeType.SUB_CONDITION, label
        display = f"[{label}] {content[:80]}..." if len(content) > 80 else f"[{label}] {content}"
        sn = self.network.create_node(nt, display, section=sec)
        self.network.create_edge(pid, sn.id, et, cond)
        for pc in sub.get('procedure_refs', []): self._add_ref(pc, sn.id)
    def _add_ref(self, pc, src):
        if pc not in self.network.procedure_refs: self.network.procedure_refs[pc] = ProcedureReference(id=f"ref_{pc}", procedure_code=pc, status=LinkStatus.PENDING.value)
        rn = self.network.create_node(NodeType.REFERENCE, f"-> {pc}", procedure_code=pc)
        self.network.create_edge(src, rn.id, EdgeType.REFERENCE)
    def _extract_entities(self, parsed):
        t = parsed.get('raw_text', '')
        for pid in re.findall(r'\b([A-Z]\d{2}[A-Z0-9]{3}[A-Z]\d{2}[A-Z0-9]{3})\b', t):
            eid = f"ent_{hashlib.md5(pid.encode()).hexdigest()[:8]}"
            if eid not in self.network.entities: self.network.entities[eid] = Entity(id=eid, name=pid, entity_type='provider_id', value=pid)


class HTMLContentParser(HTMLParser):
    def __init__(self): super().__init__(); self.text = []; self.in_body = False; self.skip = {'script', 'style'}; self.tag = None
    def handle_starttag(self, tag, attrs): self.tag = tag; self.in_body = tag == 'body' or self.in_body
    def handle_endtag(self, tag): self.in_body = False if tag == 'body' else self.in_body; self.tag = None
    def handle_data(self, data):
        if self.in_body and self.tag not in self.skip:
            t = data.strip()
            if t: self.text.append(t)
    def get_text(self): return '\n'.join(self.text)


class DeepLinkResolver:
    def __init__(self, pdir=None): self.pdir = pdir; self.parser = SOPParser(); self.builder = WorldNetworkBuilder()
    def resolve_all(self, net, max_d=3):
        if not self.pdir: return net
        pending = [c for c, r in net.procedure_refs.items() if r.status == LinkStatus.PENDING.value]
        if not pending: return net
        print(f"   Resolving {len(pending)} procedure references..."); cnt = 0
        for pc in pending:
            if self._resolve(pc, net, 0, max_d): cnt += 1
        print(f"   Resolved {cnt}/{len(pending)} procedures"); return net
    def _resolve(self, pc, net, d, max_d):
        if d >= max_d: return False
        fp = self._find(pc)
        if not fp: net.procedure_refs[pc].status = LinkStatus.NOT_FOUND.value; print(f"      {pc}: Not found"); return False
        try:
            txt = self._extract(fp); parsed = self.parser.parse(txt); info = parsed.get('document_info', {})
            sub = self.builder.build(parsed, pc, info.get('title', pc)); self._merge(net, sub, pc)
            net.procedure_refs[pc].status = LinkStatus.RESOLVED.value; net.procedure_refs[pc].source_file = fp
            print(f"      {pc}: Resolved ({len(sub.nodes)} nodes) from {os.path.basename(fp)}")
            for cc in sub.procedure_refs.keys():
                if cc not in net.procedure_refs: net.procedure_refs[cc] = ProcedureReference(id=f"ref_{cc}", procedure_code=cc, status=LinkStatus.PENDING.value)
                if net.procedure_refs[cc].status == LinkStatus.PENDING.value: self._resolve(cc, net, d + 1, max_d)
            return True
        except Exception as e: net.procedure_refs[pc].status = LinkStatus.ERROR.value; net.procedure_refs[pc].error_message = str(e); print(f"      {pc}: Error - {str(e)[:50]}"); return False
    def _find(self, pc):
        if not self.pdir or not os.path.exists(self.pdir): return None
        code_num = pc.split('.')[-1] if '.' in pc else pc
        patterns = [f"*{pc}*", f"*{pc.replace('.', '_')}*", f"*{pc.replace('.', '')}*", f"*{code_num}*", f"*PROPCL{code_num}*", f"*PR_OP_CL_{code_num}*"]
        for pat in patterns:
            for ext in ['.pdf', '.html', '.htm', '.PDF', '.HTML']:
                ms = glob.glob(os.path.join(self.pdir, "**", pat + ext), recursive=True)
                if ms: return ms[0]
        return None
    def _extract(self, fp):
        ext = os.path.splitext(fp)[1].lower()
        if ext == '.pdf':
            try: import pymupdf4llm; return pymupdf4llm.to_markdown(fp)
            except: import fitz; doc = fitz.open(fp); t = "".join(p.get_text() for p in doc); doc.close(); return t
        else:
            with open(fp, 'r', encoding='utf-8', errors='ignore') as f: content = f.read()
            p = HTMLContentParser(); p.feed(content); text = p.get_text()
            if not text.strip(): text = re.sub(r'<[^>]+>', ' ', content); text = re.sub(r'\s+', ' ', text)
            return text
    def _merge(self, main, sub, pc):
        idmap = {}; lr = main.create_node(NodeType.LINKED_PROCEDURE, f"{pc}: {sub.document_name}", procedure_code=pc)
        main.linked_procedures[pc] = lr.id
        for n in list(main.nodes.values()):
            if n.node_type == NodeType.REFERENCE and n.procedure_code == pc: main.create_edge(n.id, lr.id, EdgeType.DEEP_LINK)
        for oid, n in sub.nodes.items():
            if n.node_type == NodeType.ROOT: idmap[oid] = lr.id; continue
            nn = main.create_node(n.node_type, n.content, section=f"{pc}/{n.section}" if n.section else pc, step_number=n.step_number, procedure_code=pc)
            idmap[oid] = nn.id
        for e in sub.edges.values():
            s, t = idmap.get(e.source_id), idmap.get(e.target_id)
            if s and t: main.create_edge(s, t, e.edge_type, e.condition)
        for cn, cr in sub.claim_type_roots.items():
            nr = idmap.get(cr)
            if nr: main.claim_type_roots[f"{pc}/{cn}"] = nr


def clean_text(s):
    if not s: return ""
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', s)
    s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def generate_html(net):
    def build_tree(nid, vis=None, d=0):
        if vis is None: vis = set()
        if nid in vis or nid not in net.nodes or d > 50: return None
        vis.add(nid); n = net.nodes[nid]
        name = clean_text(n.content[:50])
        if n.step_number: name = f"S{n.step_number}: {name}"
        ch = []
        for e in net.get_outgoing_edges(nid):
            ct = build_tree(e.target_id, vis.copy(), d + 1)
            if ct: ct['edgeLabel'] = clean_text(e.condition or ''); ct['edgeType'] = e.edge_type.value; ch.append(ct)
        return {'id': nid, 'name': name, 'type': n.node_type.value, 'fullContent': clean_text(n.content[:300]), 'isLinked': n.node_type == NodeType.LINKED_PROCEDURE, 'isReference': n.node_type == NodeType.REFERENCE, 'procedureCode': n.procedure_code or '', 'children': ch}
    
    trees = {}
    for ct, rid in net.claim_type_roots.items():
        t = build_tree(rid)
        if t: trees[ct] = t
    for pc, rid in net.linked_procedures.items():
        key = f"LINKED_{pc}"
        if key not in trees:
            t = build_tree(rid)
            if t: trees[key] = t
    
    btns = []; first = True
    main_ct = sorted([c for c in net.claim_type_roots.keys() if '/' not in c])
    linked_ct = sorted([c for c in trees.keys() if c.startswith('LINKED_')])
    for c in main_ct:
        cls = "cb active" if first else "cb"; first = False
        btns.append({"name": c, "cls": cls, "isLinked": False})
    for c in linked_ct:
        btns.append({"name": c, "cls": "cb lk", "isLinked": True})
    
    refs = []
    for c, r in net.procedure_refs.items():
        status = "resolved" if r.status == "resolved" else "pending" if r.status == "pending" else "error"
        refs.append({"code": c, "status": status, "title": r.title or c})
    
    config = {"trees": trees, "buttons": btns, "refs": refs, "linkedProcedures": list(net.linked_procedures.keys())}
    config_json = json.dumps(config, ensure_ascii=True)
    
    return '''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>World Network</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#f5f7fa;overflow:hidden}
.hd{background:linear-gradient(135deg,#667eea,#764ba2);padding:12px 20px;position:fixed;top:0;left:0;right:0;z-index:100}
.hd h1{font-size:16px;color:#fff;margin-bottom:6px}
.hi{font-size:11px;color:rgba(255,255,255,.8);margin-bottom:8px}
.bs{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.sep{color:rgba(255,255,255,.4);margin:0 8px;font-weight:bold}
.cb{padding:5px 12px;border:none;background:rgba(255,255,255,.2);color:#fff;border-radius:16px;cursor:pointer;font-size:11px;transition:all 0.2s}
.cb:hover{background:rgba(255,255,255,.3);transform:scale(1.05)}
.cb.active{background:#fff;color:#667eea;font-weight:600}
.cb.lk{background:rgba(0,188,212,.3);border:1px dashed rgba(255,255,255,.5)}
.cb.lk:hover{background:rgba(0,188,212,.5)}
.cb.lk.active{background:#00bcd4;color:#fff;border-style:solid}
.lg{position:fixed;top:105px;right:15px;background:#fff;padding:12px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,.1);z-index:100;font-size:10px}
.lg-t{font-weight:600;margin-bottom:8px;color:#333}
.lg-i{display:flex;align-items:center;gap:8px;margin:5px 0}
.lg-d{width:12px;height:12px;background:#2196f3;transform:rotate(45deg);border-radius:2px}
.lg-r{width:16px;height:10px;border-radius:3px}
.prs{position:fixed;top:320px;right:15px;background:#fff;padding:12px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,.1);z-index:100;font-size:10px;max-width:200px;max-height:250px;overflow-y:auto}
.prs-t{font-weight:600;margin-bottom:8px;color:#333}
.pr{padding:4px 8px;margin:3px 0;cursor:pointer;border-radius:6px;display:flex;align-items:center;gap:6px;transition:all 0.2s}
.pr:hover{background:#e3f2fd;transform:translateX(3px)}
.pr.resolved{border-left:3px solid #4caf50}
.pr.pending{border-left:3px solid #ff9800}
.pr.error{border-left:3px solid #f44336}
.sw{margin-top:95px;overflow:auto;height:calc(100vh - 95px);cursor:grab;background:linear-gradient(135deg,#eef1f5,#e8eef5)}
#svg{display:block}
.tp{position:fixed;background:rgba(30,30,30,.95);color:#fff;padding:12px 16px;border-radius:10px;font-size:11px;max-width:400px;pointer-events:none;z-index:1000;display:none;box-shadow:0 4px 20px rgba(0,0,0,.3)}
.ct{position:fixed;bottom:20px;left:20px;display:flex;gap:8px;z-index:100}
.ctb{width:40px;height:40px;border:none;background:#fff;border-radius:50%;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.15);font-size:18px;transition:all 0.2s}
.ctb:hover{background:#667eea;color:#fff;transform:scale(1.1)}
.st{position:fixed;bottom:20px;right:20px;background:#fff;padding:10px 14px;border-radius:10px;font-size:11px;color:#666;z-index:100;box-shadow:0 2px 10px rgba(0,0,0,.1)}
.st span{font-weight:600;color:#667eea}
.bc{position:fixed;top:70px;left:20px;background:rgba(255,255,255,.9);padding:6px 12px;border-radius:20px;font-size:11px;z-index:100;display:none;box-shadow:0 2px 10px rgba(0,0,0,.1)}
.bc a{color:#667eea;text-decoration:none;cursor:pointer}
.bc a:hover{text-decoration:underline}
</style>
</head>
<body>
<div class="hd">
<h1 id="title">World Network</h1>
<div class="hi" id="info"></div>
<div class="bs" id="btns"></div>
</div>
<div class="bc" id="bc"></div>
<div class="lg">
<div class="lg-t">Legend</div>
<div class="lg-i"><div class="lg-d"></div><span>Decision</span></div>
<div class="lg-i"><div class="lg-r" style="background:#4caf50"></div><span>Yes Branch</span></div>
<div class="lg-i"><div class="lg-r" style="background:#f44336"></div><span>No Branch</span></div>
<div class="lg-i"><div class="lg-r" style="background:#ff9800"></div><span>Sub-condition</span></div>
<div class="lg-i"><div class="lg-r" style="background:#9c27b0"></div><span>Claim Type</span></div>
<div class="lg-i"><div class="lg-r" style="background:#607d8b"></div><span>Step</span></div>
<div class="lg-i"><div class="lg-r" style="background:#e91e63"></div><span>Reference</span></div>
<div class="lg-i"><div class="lg-r" style="background:#00bcd4;border:2px dashed #006064"></div><span>Linked Proc</span></div>
</div>
<div class="prs">
<div class="prs-t" id="refsTitle">Procedures</div>
<div id="refs"></div>
</div>
<div class="ct">
<button class="ctb" onclick="zI()" title="Zoom In">+</button>
<button class="ctb" onclick="zO()" title="Zoom Out">-</button>
<button class="ctb" onclick="rst()" title="Reset">R</button>
<button class="ctb" onclick="goBack()" title="Go Back" id="backBtn" style="display:none">&#8592;</button>
</div>
<div class="st">Nodes: <span id="nc">0</span> | Depth: <span id="dp">0</span></div>
<div class="tp" id="tp"></div>
<div class="sw" id="sw"><svg id="svg"></svg></div>
<script>
var CFG = ''' + config_json + ''';
var D = CFG.trees;
var T = Object.keys(D);
var C = T[0] || "";
var navHistory = [];
var nodeData = {};

function init() {
  var btnsHtml = "";
  var mainBtns = CFG.buttons.filter(function(b) { return !b.isLinked; });
  var linkedBtns = CFG.buttons.filter(function(b) { return b.isLinked; });
  
  mainBtns.forEach(function(b, i) {
    btnsHtml += "<button class='" + b.cls + "' data-tree='" + b.name + "'>" + b.name + "</button>";
  });
  
  if (linkedBtns.length > 0) {
    btnsHtml += "<span class='sep'>|</span><span style='color:rgba(255,255,255,.7);font-size:10px'>Linked:</span>";
    linkedBtns.forEach(function(b) {
      var displayName = b.name.replace("LINKED_", "");
      btnsHtml += "<button class='" + b.cls + "' data-tree='" + b.name + "'>" + displayName + "</button>";
    });
  }
  document.getElementById("btns").innerHTML = btnsHtml;
  
  document.querySelectorAll(".cb").forEach(function(btn) {
    btn.addEventListener("click", function() {
      showTree(this.getAttribute("data-tree"));
    });
  });
  
  var refsHtml = "";
  CFG.refs.forEach(function(r) {
    var icon = r.status === "resolved" ? "&#10003;" : r.status === "pending" ? "&#8230;" : "&#10007;";
    refsHtml += "<div class='pr " + r.status + "' data-code='" + r.code + "'><span>" + icon + "</span> " + r.code + "</div>";
  });
  document.getElementById("refs").innerHTML = refsHtml;
  document.getElementById("refsTitle").textContent = "Procedures (" + CFG.refs.length + ")";
  
  document.querySelectorAll(".pr.resolved").forEach(function(el) {
    el.addEventListener("click", function() {
      jumpToProc(this.getAttribute("data-code"));
    });
  });
  
  rnd();
}

var sw = document.getElementById("sw");
var svg = document.getElementById("svg");
var tp = document.getElementById("tp");
var bc = document.getElementById("bc");
var backBtn = document.getElementById("backBtn");
var sc = 0.6;
var NW = 170, NH = 38, LH = 95, NS = 25, DS = 26;
var col = {
  root: "#4caf50", claim_type: "#9c27b0", decision: "#2196f3",
  branch_yes: "#4caf50", branch_no: "#f44336", branch_unsure: "#ff9800",
  sub_condition: "#ff9800", step: "#607d8b", reference: "#e91e63", linked_procedure: "#00bcd4"
};

function lay(n, l, lo) {
  if (!n) return {w: 0, nodes: [], edges: [], d: 0};
  var ch = n.children || [];
  var nodes = [], edges = [], md = l;
  if (!ch.length) {
    nodes.push(Object.assign({}, n, {x: lo + NW/2, y: l * LH + 60}));
    return {w: NW + NS, nodes: nodes, edges: edges, d: l};
  }
  var co = lo, cr = [];
  for (var i = 0; i < ch.length; i++) {
    var r = lay(ch[i], l + 1, co);
    cr.push(r); nodes = nodes.concat(r.nodes); edges = edges.concat(r.edges);
    co += r.w; md = Math.max(md, r.d);
  }
  var tw = 0; for (var j = 0; j < cr.length; j++) tw += cr[j].w;
  var fx = cr[0].nodes[0] ? cr[0].nodes[0].x : lo;
  var lx = cr[cr.length-1].nodes[0] ? cr[cr.length-1].nodes[0].x : lo;
  var cx = (fx + lx) / 2, y = l * LH + 60;
  nodes.unshift(Object.assign({}, n, {x: cx, y: y}));
  for (var k = 0; k < ch.length; k++) {
    var cn = null;
    for (var m = 0; m < nodes.length; m++) if (nodes[m].id === ch[k].id) { cn = nodes[m]; break; }
    if (cn) {
      edges.push({fx: cx, fy: y, tx: cn.x, ty: cn.y, lb: ch[k].edgeLabel || "", pt: n.type, et: ch[k].edgeType || "", dl: ch[k].type === "linked_procedure" || ch[k].edgeType === "deep_link"});
    }
  }
  return {w: Math.max(tw, NW + NS), nodes: nodes, edges: edges, d: md};
}

function rnd() {
  var t = D[C];
  if (!t) { svg.innerHTML = "<text x='50' y='100' fill='#666'>No data</text>"; return; }
  var r = lay(t, 0, 80);
  var nodes = r.nodes, edges = r.edges, d = r.d;
  document.getElementById("nc").textContent = nodes.length;
  document.getElementById("dp").textContent = d;
  
  nodeData = {};
  nodes.forEach(function(n) { nodeData[n.id] = n; });
  
  var mx = 0, my = 0;
  nodes.forEach(function(n) { mx = Math.max(mx, n.x + NW); my = Math.max(my, n.y + NH + 60); });
  var w = Math.max(mx + 150, 900), h = Math.max(my + 100, 700);
  svg.setAttribute("width", w * sc);
  svg.setAttribute("height", h * sc);
  svg.setAttribute("viewBox", "0 0 " + w + " " + h);
  
  var s = "<defs><filter id='sh'><feDropShadow dx='2' dy='3' stdDeviation='3' flood-opacity='0.15'/></filter></defs>";
  
  edges.forEach(function(e) {
    var isDec = e.pt === "decision";
    var fy = e.fy + (isDec ? DS/2 + 8 : NH/2);
    var ty = e.ty - NH/2 - 4;
    var my2 = (fy + ty) / 2;
    var stk = e.dl ? "#00bcd4" : "#aaa";
    var sw2 = e.dl ? 3 : 2;
    var da = e.dl ? "6,4" : "none";
    s += "<path d='M " + e.fx + " " + fy + " C " + e.fx + " " + my2 + "," + e.tx + " " + my2 + "," + e.tx + " " + ty + "' fill='none' stroke='" + stk + "' stroke-width='" + sw2 + "' stroke-dasharray='" + da + "'/>";
    s += "<circle cx='" + e.tx + "' cy='" + ty + "' r='3' fill='" + stk + "'/>";
    if (e.lb) {
      var lx = (e.fx + e.tx) / 2, ly = my2 - 6;
      var lc = e.lb === "YES" ? "#2e7d32" : e.lb === "NO" ? "#c62828" : "#ef6c00";
      s += "<rect x='" + (lx - 18) + "' y='" + (ly - 10) + "' width='36' height='14' rx='7' fill='" + lc + "'/>";
      s += "<text x='" + lx + "' y='" + ly + "' text-anchor='middle' font-size='9' font-weight='600' fill='#fff'>" + e.lb + "</text>";
    }
  });
  
  nodes.forEach(function(n) {
    var c = col[n.type] || col.step;
    var isDec = n.type === "decision";
    var isLnk = n.type === "linked_procedure";
    var isRef = n.type === "reference";
    var lb = n.name.length > 24 ? n.name.slice(0, 24) + "..." : n.name;
    var isResolved = isRef && n.procedureCode && CFG.linkedProcedures.indexOf(n.procedureCode) >= 0;
    
    if (isRef && !isResolved) c = "#9e9e9e";
    
    s += "<g class='node' data-id='" + n.id + "' data-clickable='" + (isResolved || isLnk ? "1" : "0") + "' data-pc='" + (n.procedureCode || "") + "' style='cursor:" + ((isResolved || isLnk) ? "pointer" : "default") + "'>";
    
    if (isDec) {
      s += "<rect x='" + (n.x - DS/2) + "' y='" + (n.y - DS/2) + "' width='" + DS + "' height='" + DS + "' fill='" + c + "' stroke='#fff' stroke-width='2' rx='4' transform='rotate(45 " + n.x + " " + n.y + ")' filter='url(#sh)'/>";
      s += "<text x='" + n.x + "' y='" + (n.y + DS/2 + 16) + "' text-anchor='middle' font-size='9' fill='#333' font-weight='500'>" + lb + "</text>";
    } else if (isLnk) {
      s += "<rect x='" + (n.x - NW/2) + "' y='" + (n.y - NH/2) + "' width='" + NW + "' height='" + NH + "' fill='" + c + "' stroke='#006064' stroke-width='3' stroke-dasharray='6,3' rx='10' filter='url(#sh)'/>";
      s += "<text x='" + n.x + "' y='" + (n.y + 1) + "' text-anchor='middle' dominant-baseline='middle' font-size='10' fill='#fff' font-weight='600'>[+] " + lb + "</text>";
    } else if (isRef) {
      s += "<rect x='" + (n.x - NW/2) + "' y='" + (n.y - NH/2) + "' width='" + NW + "' height='" + NH + "' fill='" + c + "' stroke='" + (isResolved ? "#880e4f" : "#666") + "' stroke-width='2' rx='10' filter='url(#sh)'/>";
      var refLabel = n.procedureCode || n.name;
      if (isResolved) {
        s += "<text x='" + n.x + "' y='" + (n.y - 4) + "' text-anchor='middle' font-size='8' fill='#fff'>CLICK TO OPEN</text>";
      }
      s += "<text x='" + n.x + "' y='" + (n.y + (isResolved ? 10 : 1)) + "' text-anchor='middle' font-size='10' fill='#fff' font-weight='bold'>" + refLabel + "</text>";
    } else {
      s += "<rect x='" + (n.x - NW/2) + "' y='" + (n.y - NH/2) + "' width='" + NW + "' height='" + NH + "' fill='" + c + "' stroke='#fff' stroke-width='2' rx='10' filter='url(#sh)'/>";
      s += "<text x='" + n.x + "' y='" + (n.y + 1) + "' text-anchor='middle' dominant-baseline='middle' font-size='9' fill='#fff' font-weight='500'>" + lb + "</text>";
    }
    s += "</g>";
  });
  
  svg.innerHTML = s;
  
  document.querySelectorAll(".node").forEach(function(el) {
    el.addEventListener("mouseover", function(e) { stp(e, this.getAttribute("data-id")); });
    el.addEventListener("mouseout", htp);
    if (el.getAttribute("data-clickable") === "1") {
      el.addEventListener("click", function() {
        var pc = this.getAttribute("data-pc");
        if (pc) jumpToProc(pc);
      });
    }
  });
  
  backBtn.style.display = navHistory.length > 0 ? "block" : "none";
  
  if (navHistory.length > 0) {
    var bcHtml = "<a onclick='goHome()'>Main</a>";
    navHistory.forEach(function(h, i) {
      bcHtml += " &gt; <a onclick='goToHistory(" + i + ")'>" + h.replace("LINKED_", "") + "</a>";
    });
    bcHtml += " &gt; " + C.replace("LINKED_", "");
    bc.innerHTML = bcHtml;
    bc.style.display = "block";
  } else {
    bc.style.display = "none";
  }
}

function stp(e, id) {
  var n = nodeData[id];
  if (!n) return;
  var h = "<div style='color:#64b5f6;font-weight:600;margin-bottom:4px'>" + n.type.replace(/_/g, " ").toUpperCase() + "</div>";
  if (n.procedureCode) h += "<div style='color:#e91e63;font-size:10px'>Procedure: " + n.procedureCode + "</div>";
  h += "<div style='margin-top:6px'>" + (n.fullContent || n.name) + "</div>";
  if (n.isReference && n.procedureCode) {
    var isResolved = CFG.linkedProcedures.indexOf(n.procedureCode) >= 0;
    if (isResolved) {
      h += "<div style='margin-top:8px;color:#4caf50;font-weight:600'>Click to view procedure tree</div>";
    } else {
      h += "<div style='margin-top:8px;color:#ff9800'>Procedure not loaded - run with --deep-link-dir</div>";
    }
  }
  tp.innerHTML = h;
  tp.style.display = "block";
  var x = e.clientX + 15, y = e.clientY + 15;
  if (x + 400 > window.innerWidth) x = e.clientX - 400;
  if (y + 150 > window.innerHeight) y = e.clientY - 150;
  tp.style.left = x + "px"; tp.style.top = y + "px";
}

function htp() { tp.style.display = "none"; }

function showTree(name) {
  history = [];
  C = name;
  updateButtons();
  rst();
}

function jumpToProc(pc) {
  var targetKey = "LINKED_" + pc;
  if (D[targetKey]) {
    navHistory.push(C);
    C = targetKey;
    updateButtons();
    rst();
  } else {
    alert("Procedure " + pc + " is not loaded.\\n\\nRun with: --deep-link-dir <folder>");
  }
}

function goBack() {
  if (navHistory.length > 0) {
    C = navHistory.pop();
    updateButtons();
    rst();
  }
}

function goHome() {
  history = [];
  C = T[0] || "";
  updateButtons();
  rst();
}

function goToHistory(idx) {
  C = navHistory[idx];
  history = navHistory.slice(0, idx);
  updateButtons();
  rst();
}

function updateButtons() {
  document.querySelectorAll(".cb").forEach(function(btn) {
    var btnTree = btn.getAttribute("data-tree");
    btn.classList.toggle("active", btnTree === C);
  });
}

function zI() { sc = Math.min(sc * 1.25, 3); rnd(); }
function zO() { sc = Math.max(sc / 1.25, 0.15); rnd(); }
function rst() { sc = 0.6; sw.scrollTo(0, 0); rnd(); }

document.addEventListener("keydown", function(e) {
  if (e.key === "+" || e.key === "=") zI();
  else if (e.key === "-") zO();
  else if (e.key === "0") rst();
  else if (e.key === "Backspace" || e.key === "Escape") goBack();
});

sw.addEventListener("wheel", function(e) {
  if (e.ctrlKey) { e.preventDefault(); e.deltaY < 0 ? zI() : zO(); }
});

init();
</script>
</body>
</html>'''


class WorldNetworkProcessor:
    def __init__(self, pdir=None):
        self.parser = SOPParser(); self.builder = WorldNetworkBuilder()
        self.resolver = DeepLinkResolver(pdir); self.pdir = pdir
    def process(self, pdf, outdir, max_d=3):
        print("=" * 70); print("WORLD NETWORK BUILDER"); print("=" * 70)
        os.makedirs(outdir, exist_ok=True)
        print(f"\n[1] Extracting PDF..."); txt = self._extract(pdf); print(f"    {len(txt):,} chars")
        print(f"\n[2] Parsing..."); parsed = self.parser.parse(txt)
        info = parsed.get('document_info', {}); doc_id = info.get('document_id', 'UNK'); doc_name = info.get('title', 'Untitled')
        print(f"    {doc_name} ({doc_id})"); print(f"    {len(parsed.get('sections', []))} sections, {len(parsed.get('procedure_references', []))} refs")
        print(f"\n[3] Building network..."); net = self.builder.build(parsed, doc_id, doc_name)
        print(f"    {len(net.nodes)} nodes, {len(net.edges)} edges")
        if self.pdir:
            print(f"\n[4] Deep linking from {self.pdir}..."); net = self.resolver.resolve_all(net, max_d)
            print(f"    Total: {len(net.nodes)} nodes, {len(net.linked_procedures)} linked")
        else: print(f"\n[4] Deep linking: SKIPPED (use --deep-link-dir)")
        print(f"\n[5] Saving...")
        with open(os.path.join(outdir, 'world_network.json'), 'w', encoding='utf-8') as f: json.dump(net.to_dict(), f, indent=2, ensure_ascii=False)
        with open(os.path.join(outdir, 'world_network_tree.html'), 'w', encoding='utf-8') as f: f.write(generate_html(net))
        print(f"\n" + "=" * 70); print("COMPLETE!"); print(f"Output: {outdir}")
        if net.linked_procedures: print(f"Linked: {list(net.linked_procedures.keys())}")
        return net
    def _extract(self, pdf):
        try: import pymupdf4llm; return pymupdf4llm.to_markdown(pdf)
        except: import fitz; doc = fitz.open(pdf); t = "".join(p.get_text() for p in doc); doc.close(); return t


def main():
    import argparse
    p = argparse.ArgumentParser(description='World Network Builder')
    p.add_argument('pdf', help='Input PDF'); p.add_argument('outdir', help='Output dir')
    p.add_argument('--deep-link-dir', '-d', dest='pdir', help='Procedures folder')
    p.add_argument('--max-depth', '-m', type=int, default=3, help='Max depth')
    a = p.parse_args()
    if not os.path.exists(a.pdf): print(f"Error: {a.pdf} not found"); sys.exit(1)
    if a.pdir and not os.path.exists(a.pdir): print(f"Warning: {a.pdir} not found"); a.pdir = None
    WorldNetworkProcessor(pdir=a.pdir).process(a.pdf, a.outdir, a.max_depth)


if __name__ == "__main__": main()
