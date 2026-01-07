#!/usr/bin/env python3
"""
World Network Builder - Complete Script with Deep Linking
Usage: python world_network_complete.py <pdf> <output_dir> [--deep-link-dir <dir>]
"""
import re,json,hashlib,sys,os,glob
from dataclasses import dataclass,field,asdict
from typing import Dict,List,Optional,Any
from enum import Enum

class NodeType(Enum):
    ROOT="root";CLAIM_TYPE="claim_type";DECISION="decision";ACTION="action"
    BRANCH_YES="branch_yes";BRANCH_NO="branch_no";BRANCH_UNSURE="branch_unsure"
    REFERENCE="reference";LINKED_PROCEDURE="linked_procedure";STEP="step"

class EdgeType(Enum):
    SEQUENCE="sequence";CONDITION_YES="condition_yes";CONDITION_NO="condition_no"
    CONDITION_UNSURE="condition_unsure";REFERENCE="reference";DEEP_LINK="deep_link";CONTAINS="contains"

class LinkStatus(Enum):
    PENDING="pending";RESOLVED="resolved";NOT_FOUND="not_found";ERROR="error"

@dataclass
class Entity:
    id:str;name:str;entity_type:str;value:str=""
    def to_dict(self):return asdict(self)

@dataclass
class NetworkNode:
    id:str;node_type:NodeType;content:str;section:Optional[str]=None
    step_number:Optional[int]=None;metadata:Dict[str,Any]=field(default_factory=dict)
    procedure_code:Optional[str]=None
    def to_dict(self):d=asdict(self);d['node_type']=self.node_type.value;return d

@dataclass
class NetworkEdge:
    id:str;source_id:str;target_id:str;edge_type:EdgeType;condition:Optional[str]=None
    def to_dict(self):d=asdict(self);d['edge_type']=self.edge_type.value;return d

@dataclass
class ProcedureReference:
    id:str;procedure_code:str;title:str="";status:str="pending"
    source_file:Optional[str]=None;error_message:Optional[str]=None
    def to_dict(self):return asdict(self)

@dataclass
class VersionInfo:
    revision:str;date:str;description:str=""
    def to_dict(self):return asdict(self)

class WorldNetwork:
    def __init__(self,doc_id:str,doc_name:str):
        self.document_id=doc_id;self.document_name=doc_name;self.current_version="1.0"
        self.nodes:Dict[str,NetworkNode]={};self.edges:Dict[str,NetworkEdge]={}
        self.entities:Dict[str,Entity]={};self.procedure_refs:Dict[str,ProcedureReference]={}
        self.versions:List[VersionInfo]=[];self.claim_type_roots:Dict[str,str]={}
        self.linked_procedures:Dict[str,str]={};self.metadata:Dict[str,Any]={}
        self._nc=0;self._ec=0
    def create_node(self,nt:NodeType,content:str,**kw)->NetworkNode:
        self._nc+=1;nid=f"node_{self._nc:04d}"
        n=NetworkNode(id=nid,node_type=nt,content=content,**kw);self.nodes[nid]=n;return n
    def create_edge(self,src:str,tgt:str,et:EdgeType,cond:Optional[str]=None)->NetworkEdge:
        self._ec+=1;eid=f"edge_{self._ec:04d}"
        e=NetworkEdge(id=eid,source_id=src,target_id=tgt,edge_type=et,condition=cond);self.edges[eid]=e;return e
    def get_outgoing_edges(self,nid:str)->List[NetworkEdge]:
        return[e for e in self.edges.values()if e.source_id==nid]
    def to_dict(self)->Dict:
        return{'document_id':self.document_id,'document_name':self.document_name,'current_version':self.current_version,
               'nodes':{k:v.to_dict()for k,v in self.nodes.items()},'edges':{k:v.to_dict()for k,v in self.edges.items()},
               'entities':{k:v.to_dict()for k,v in self.entities.items()},'procedure_refs':{k:v.to_dict()for k,v in self.procedure_refs.items()},
               'versions':[v.to_dict()for v in self.versions],'claim_type_roots':self.claim_type_roots,
               'linked_procedures':self.linked_procedures,'metadata':self.metadata}

class SOPParser:
    PATTERNS=[r'###\s*\*\*\s*(Amazon\s+Claims?)\s*\*\*',r'###\s*\*\*\s*(Alaska\s+Air.*?Claims?)\s*\*\*',
              r'###\s*\*\*\s*(Microsoft\s+Claims?)\s*\*\*',r'###\s*\*\*\s*(Expedia\s+Claims?)\s*\*\*',
              r'###\s*\*\*\s*(FEP\s+Claims?)\s*\*\*',r'###\s*\*\*\s*(LEOFF.*?)\s*\*\*',r'###\s*\*\*\s*(All\s+Others?)\s*\*\*',
              r'^#+\s*(Amazon\s+Claims?)',r'^#+\s*(Microsoft\s+Claims?)',r'^#+\s*(All\s+Others?)']
    STEP_PAT=re.compile(r'^(\d+)\.\s*(.+)',re.MULTILINE)
    DEC_PAT=re.compile(r'^(?:Is|Does|Did|Are|Has|Have|Was|Were|Can|Should)\s+',re.IGNORECASE)
    PROC_PAT=re.compile(r'(PR\.OP\.CL\.\d{4})')
    VER_PAT=re.compile(r'^\|\s*(\d+\.\d+)\s*\|\s*(\d{1,2}/\d{1,2}/\d{4}[^|]*)\s*\|\s*([^|]+)\s*\|',re.MULTILINE)
    
    def parse(self,text:str)->Dict:
        return{'document_info':self._doc_info(text),'versions':self._versions(text),'sections':self._sections(text),
               'procedure_references':self._all_refs(text),'raw_text':text}
    def _doc_info(self,t):
        info={'title':'','document_id':'','status':''}
        m=re.search(r'^#\s+(.+?)(?:\n|$)',t,re.MULTILINE);info['title']=m.group(1).strip()if m else''
        m=re.search(r'\b(P\d{3,4})\b',t);info['document_id']=m.group(1)if m else''
        if'CURRENT'in t.upper():info['status']='Current'
        return info
    def _versions(self,t):
        return[{'revision':m.group(1),'date':m.group(2).strip(),'description':m.group(3).strip()}for m in self.VER_PAT.finditer(t)]
    def _sections(self,t):
        matches=[];seen=set()
        for p in self.PATTERNS:
            for m in re.finditer(p,t,re.MULTILINE|re.IGNORECASE):
                n=m.group(1).strip().lower()
                if n not in seen:seen.add(n);matches.append((m.start(),m.group(1).strip()))
        matches.sort(key=lambda x:x[0]);secs=[]
        for i,(pos,name)in enumerate(matches):
            end=matches[i+1][0]if i+1<len(matches)else len(t)
            txt=t[pos:end]
            secs.append({'name':name,'steps':self._steps(txt),'procedure_refs':list(set(self.PROC_PAT.findall(txt)))})
        return secs
    def _steps(self,t):
        steps=[];ms=list(self.STEP_PAT.finditer(t))
        for i,m in enumerate(ms):
            end=ms[i+1].start()if i+1<len(ms)else len(t)
            txt=t[m.start():end].strip();content=m.group(2).strip()
            is_dec=bool(self.DEC_PAT.search(content))or'?'in content
            steps.append({'number':int(m.group(1)),'content':content,'full_text':txt,'is_decision':is_dec,
                         'branches':self._branches(txt)if is_dec else[],'procedure_refs':list(set(self.PROC_PAT.findall(txt)))})
        return steps
    def _branches(self,t):
        branches=[];cur=None
        for ln in t.split('\n'):
            ln=ln.strip()
            if not ln:continue
            ym=re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Yes)\s*[:\*\*]*\s*(.*)',ln,re.IGNORECASE)
            nm=re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(No)\s*[:\*\*]*\s*(.*)',ln,re.IGNORECASE)
            um=re.match(r'^\s*[-*]?\s*\*?\*?(?:I\s+)?(Unsure)\s*[:\*\*]*\s*(.*)',ln,re.IGNORECASE)
            if ym:
                if cur:branches.append(cur)
                cur={'type':'yes','content':ym.group(2).strip(),'procedure_refs':[]}
            elif nm:
                if cur:branches.append(cur)
                cur={'type':'no','content':nm.group(2).strip(),'procedure_refs':[]}
            elif um:
                if cur:branches.append(cur)
                cur={'type':'unsure','content':um.group(2).strip(),'procedure_refs':[]}
            elif cur:cur['content']+=' '+ln
        if cur:branches.append(cur)
        for b in branches:b['procedure_refs']=list(set(self.PROC_PAT.findall(b['content'])))
        return branches
    def _all_refs(self,t):
        seen=set();refs=[]
        for m in self.PROC_PAT.finditer(t):
            c=m.group(1)
            if c in seen:continue
            seen.add(c);tm=re.search(rf'{c}\s*[-:]\s*([^.\n]+)',t)
            refs.append({'code':c,'title':tm.group(1).strip()if tm else''})
        return refs

class WorldNetworkBuilder:
    def __init__(self):self.network=None
    def build(self,parsed:Dict,doc_id:str,doc_name:str)->WorldNetwork:
        self.network=WorldNetwork(doc_id,doc_name)
        info=parsed.get('document_info',{})
        self.network.metadata={'title':info.get('title',''),'status':info.get('status','')}
        for v in parsed.get('versions',[]):
            self.network.versions.append(VersionInfo(revision=v.get('revision',''),date=v.get('date',''),description=v.get('description','')))
        if self.network.versions:self.network.current_version=self.network.versions[0].revision
        root=self.network.create_node(NodeType.ROOT,doc_name)
        for sec in parsed.get('sections',[]):self._proc_section(sec,root.id)
        for ref in parsed.get('procedure_references',[]):
            c=ref['code']
            if c not in self.network.procedure_refs:
                self.network.procedure_refs[c]=ProcedureReference(id=f"ref_{c}",procedure_code=c,title=ref.get('title',''),status=LinkStatus.PENDING.value)
        self._extract_entities(parsed)
        return self.network
    def _proc_section(self,sec:Dict,pid:str):
        name=sec['name']
        ct=self.network.create_node(NodeType.CLAIM_TYPE,name,section=name)
        self.network.create_edge(pid,ct.id,EdgeType.CONTAINS)
        self.network.claim_type_roots[name]=ct.id
        prev=ct.id
        for step in sec.get('steps',[]):prev=self._proc_step(step,ct.id,prev,name)
    def _proc_step(self,step:Dict,sid:str,prev:str,sec:str)->str:
        num,content,is_dec=step['number'],step['content'],step['is_decision']
        if is_dec:
            dn=self.network.create_node(NodeType.DECISION,content,section=sec,step_number=num)
            self.network.create_edge(prev,dn.id,EdgeType.SEQUENCE)
            for b in step.get('branches',[]):self._proc_branch(b,dn.id,sec)
            for pc in step.get('procedure_refs',[]):self._add_ref(pc,dn.id,step.get('full_text',''))
            return dn.id
        else:
            sn=self.network.create_node(NodeType.STEP,content,section=sec,step_number=num)
            self.network.create_edge(prev,sn.id,EdgeType.SEQUENCE)
            for pc in step.get('procedure_refs',[]):self._add_ref(pc,sn.id,step.get('full_text',''))
            return sn.id
    def _proc_branch(self,b:Dict,pid:str,sec:str)->str:
        bt=b['type'].lower();content=b['content']
        if bt=='yes':nt,et,cond=NodeType.BRANCH_YES,EdgeType.CONDITION_YES,"YES"
        elif bt=='no':nt,et,cond=NodeType.BRANCH_NO,EdgeType.CONDITION_NO,"NO"
        else:nt,et,cond=NodeType.BRANCH_UNSURE,EdgeType.CONDITION_UNSURE,"UNSURE"
        bn=self.network.create_node(nt,content,section=sec)
        self.network.create_edge(pid,bn.id,et,cond)
        for pc in b.get('procedure_refs',[]):self._add_ref(pc,bn.id,content)
        return bn.id
    def _add_ref(self,pc:str,src:str,ctx:str):
        if pc not in self.network.procedure_refs:
            tm=re.search(rf'{pc}\s*[-:]\s*([^.\n]+)',ctx)
            self.network.procedure_refs[pc]=ProcedureReference(id=f"ref_{pc}",procedure_code=pc,title=tm.group(1).strip()if tm else'',status=LinkStatus.PENDING.value)
        rn=self.network.create_node(NodeType.REFERENCE,f"Refer to: {pc}",procedure_code=pc)
        self.network.create_edge(src,rn.id,EdgeType.REFERENCE)
    def _extract_entities(self,parsed:Dict):
        t=parsed.get('raw_text','')
        for pid in re.findall(r'\b([A-Z]\d{2}[A-Z0-9]{3}[A-Z]\d{2}[A-Z0-9]{3})\b',t):
            eid=f"ent_{hashlib.md5(pid.encode()).hexdigest()[:8]}"
            if eid not in self.network.entities:self.network.entities[eid]=Entity(id=eid,name=pid,entity_type='provider_id',value=pid)

class DeepLinkResolver:
    def __init__(self,pdir:Optional[str]=None):
        self.pdir=pdir;self.parser=SOPParser();self.builder=WorldNetworkBuilder()
    def resolve_all(self,net:WorldNetwork,max_d:int=3)->WorldNetwork:
        if not self.pdir:return net
        pending=[c for c,r in net.procedure_refs.items()if r.status==LinkStatus.PENDING.value]
        if not pending:return net
        print(f"   Resolving {len(pending)} refs...")
        cnt=0
        for pc in pending:
            if self._resolve(pc,net,0,max_d):cnt+=1
        print(f"   Resolved {cnt}/{len(pending)}")
        return net
    def _resolve(self,pc:str,net:WorldNetwork,d:int,max_d:int)->bool:
        if d>=max_d:return False
        pdf=self._find(pc)
        if not pdf:net.procedure_refs[pc].status=LinkStatus.NOT_FOUND.value;print(f"      {pc}: Not found");return False
        try:
            txt=self._extract(pdf);parsed=self.parser.parse(txt)
            info=parsed.get('document_info',{})
            sub=self.builder.build(parsed,pc,info.get('title',pc))
            self._merge(net,sub,pc)
            net.procedure_refs[pc].status=LinkStatus.RESOLVED.value
            net.procedure_refs[pc].source_file=pdf
            print(f"      {pc}: OK ({len(sub.nodes)} nodes)")
            for cc in sub.procedure_refs.keys():
                if cc not in net.procedure_refs:
                    net.procedure_refs[cc]=ProcedureReference(id=f"ref_{cc}",procedure_code=cc,status=LinkStatus.PENDING.value)
                if net.procedure_refs[cc].status==LinkStatus.PENDING.value:
                    self._resolve(cc,net,d+1,max_d)
            return True
        except Exception as e:
            net.procedure_refs[pc].status=LinkStatus.ERROR.value
            net.procedure_refs[pc].error_message=str(e)
            print(f"      {pc}: Error - {str(e)[:40]}")
            return False
    def _find(self,pc:str)->Optional[str]:
        if not self.pdir or not os.path.exists(self.pdir):return None
        for pat in[f"*{pc}*.pdf",f"*{pc.replace('.','_')}*.pdf",f"*{pc.split('.')[-1]}*.pdf"]:
            ms=glob.glob(os.path.join(self.pdir,"**",pat),recursive=True)
            if ms:return ms[0]
        return None
    def _extract(self,pdf:str)->str:
        try:import pymupdf4llm;return pymupdf4llm.to_markdown(pdf)
        except:import fitz;doc=fitz.open(pdf);t="".join(p.get_text()for p in doc);doc.close();return t
    def _merge(self,main:WorldNetwork,sub:WorldNetwork,pc:str):
        idmap={}
        lr=main.create_node(NodeType.LINKED_PROCEDURE,f"{pc}: {sub.document_name}",procedure_code=pc)
        main.linked_procedures[pc]=lr.id
        for n in main.nodes.values():
            if n.node_type==NodeType.REFERENCE and n.procedure_code==pc:
                main.create_edge(n.id,lr.id,EdgeType.DEEP_LINK)
        for oid,n in sub.nodes.items():
            if n.node_type==NodeType.ROOT:idmap[oid]=lr.id;continue
            nn=main.create_node(n.node_type,n.content,section=f"{pc}/{n.section}"if n.section else pc,step_number=n.step_number,procedure_code=pc)
            idmap[oid]=nn.id
        for e in sub.edges.values():
            s,t=idmap.get(e.source_id),idmap.get(e.target_id)
            if s and t:main.create_edge(s,t,e.edge_type,e.condition)
        for cn,cr in sub.claim_type_roots.items():
            nr=idmap.get(cr)
            if nr:main.claim_type_roots[f"{pc}/{cn}"]=nr

def generate_html(net:WorldNetwork)->str:
    def build(nid,vis=None,d=0):
        if vis is None:vis=set()
        if nid in vis or nid not in net.nodes or d>50:return None
        vis.add(nid);n=net.nodes[nid]
        lb=n.content[:40].replace('"','&#34;').replace('<','&lt;').replace('>','&gt;').replace('\n',' ').replace("'","&#39;")
        if n.step_number:lb=f"S{n.step_number}: {lb}"
        ch=[]
        for e in net.get_outgoing_edges(nid):
            ct=build(e.target_id,vis.copy(),d+1)
            if ct:ct['edgeLabel']=(e.condition or'').replace('"','&#34;').replace("'","&#39;");ch.append(ct)
        return{'id':nid,'name':lb,'type':n.node_type.value,
               'fullContent':n.content[:200].replace('"','&#34;').replace('<','&lt;').replace('>','&gt;').replace('\n',' ').replace("'","&#39;"),
               'isLinked':n.node_type==NodeType.LINKED_PROCEDURE,'procedureCode':n.procedure_code or'','children':ch}
    trees={}
    for ct,rid in net.claim_type_roots.items():
        t=build(rid)
        if t:trees[ct]=t
    for pc,rid in net.linked_procedures.items():
        if pc not in trees:
            t=build(rid)
            if t:trees[f"[Link] {pc}"]=t
    tj=json.dumps(trees,ensure_ascii=False).replace('</script>','<\\/script>').replace("'","\\'")
    btns="";first=True
    main_ct=[c for c in net.claim_type_roots.keys()if'/'not in c]
    link_ct=[c for c in trees.keys()if c.startswith('[Link]')or'/'in c]
    for c in main_ct:
        act=" active"if first else"";first=False
        sc=c.replace("'","\\'")
        btns+=f'<button class="cb{act}" onclick="show(\'{sc}\')">{c}</button>'
    if link_ct:
        btns+='<span class="sep">|</span>'
        for c in link_ct:
            sc=c.replace("'","\\'")
            btns+=f'<button class="cb lk" onclick="show(\'{sc}\')">{c}</button>'
    prefs="".join(f'<div class="pr" onclick="jump(\'{c}\')">{"&#x2705;"if r.status=="resolved"else"&#x23F3;"} {c}</div>'for c,r in net.procedure_refs.items())
    
    return f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><title>World Network - {net.document_name}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#f5f7fa;overflow:hidden}}
.hd{{background:linear-gradient(135deg,#667eea,#764ba2);padding:12px 20px;position:fixed;top:0;left:0;right:0;z-index:100}}
.hd h1{{font-size:16px;color:#fff;margin-bottom:6px}}.hi{{font-size:11px;color:rgba(255,255,255,.8);margin-bottom:8px}}
.bs{{display:flex;gap:6px;flex-wrap:wrap;align-items:center}}.sep{{color:rgba(255,255,255,.4);margin:0 4px}}
.cb{{padding:5px 12px;border:none;background:rgba(255,255,255,.2);color:#fff;border-radius:16px;cursor:pointer;font-size:11px;transition:.3s}}
.cb:hover{{background:rgba(255,255,255,.3)}}.cb.active{{background:#fff;color:#667eea}}
.cb.lk{{background:rgba(233,30,99,.3);border:1px dashed rgba(255,255,255,.5)}}.cb.lk.active{{background:#e91e63;color:#fff}}
.lg{{position:fixed;top:105px;right:15px;background:#fff;padding:10px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,.1);z-index:100;font-size:10px}}
.lg-t{{font-weight:600;margin-bottom:6px}}.lg-i{{display:flex;align-items:center;gap:6px;margin:4px 0}}
.lg-d{{width:10px;height:10px;background:#2196f3;transform:rotate(45deg);border-radius:2px}}.lg-r{{width:14px;height:8px;border-radius:3px}}
.prs{{position:fixed;top:280px;right:15px;background:#fff;padding:10px;border-radius:10px;box-shadow:0 4px 15px rgba(0,0,0,.1);z-index:100;font-size:10px;max-width:160px;max-height:200px;overflow-y:auto}}
.prs-t{{font-weight:600;margin-bottom:6px}}.pr{{padding:3px 6px;margin:2px 0;cursor:pointer;border-radius:4px}}.pr:hover{{background:#f0f0f0}}
.sw{{margin-top:95px;overflow:auto;height:calc(100vh - 95px);cursor:grab;background:#eef1f5}}
#svg{{display:block}}
.tp{{position:fixed;background:rgba(30,30,30,.95);color:#fff;padding:10px 14px;border-radius:8px;font-size:11px;max-width:350px;pointer-events:none;z-index:1000;display:none}}
.tp b{{color:#64b5f6}}.tp .pc{{color:#e91e63}}
.ct{{position:fixed;bottom:20px;left:20px;display:flex;gap:8px;z-index:100}}
.ctb{{width:36px;height:36px;border:none;background:#fff;border-radius:50%;cursor:pointer;box-shadow:0 2px 10px rgba(0,0,0,.15);font-size:18px;display:flex;align-items:center;justify-content:center}}
.ctb:hover{{background:#667eea;color:#fff}}
.st{{position:fixed;bottom:20px;right:20px;background:#fff;padding:8px 12px;border-radius:10px;font-size:10px;color:#666;z-index:100}}.st span{{font-weight:600;color:#667eea}}
</style></head><body>
<div class="hd"><h1>&#x1F333; {net.document_name}</h1>
<div class="hi">Doc: {net.document_id} | Ver: {net.current_version} | Nodes: {len(net.nodes)} | Links: {len(net.linked_procedures)}</div>
<div class="bs">{btns}</div></div>
<div class="lg"><div class="lg-t">Node Types</div>
<div class="lg-i"><div class="lg-d"></div>Decision</div>
<div class="lg-i"><div class="lg-r" style="background:#4caf50"></div>Yes</div>
<div class="lg-i"><div class="lg-r" style="background:#f44336"></div>No</div>
<div class="lg-i"><div class="lg-r" style="background:#ff9800"></div>Unsure</div>
<div class="lg-i"><div class="lg-r" style="background:#9c27b0"></div>ClaimType</div>
<div class="lg-i"><div class="lg-r" style="background:#607d8b"></div>Step</div>
<div class="lg-i"><div class="lg-r" style="background:#e91e63"></div>Reference</div>
<div class="lg-i"><div class="lg-r" style="background:#00bcd4;border:2px dashed #006064"></div>Linked</div></div>
<div class="prs"><div class="prs-t">Procedure Links ({len(net.procedure_refs)})</div>{prefs}</div>
<div class="ct"><button class="ctb" onclick="zI()">+</button><button class="ctb" onclick="zO()">-</button><button class="ctb" onclick="rst()">&#x21BA;</button></div>
<div class="st">Nodes: <span id="nc">0</span> | Depth: <span id="dp">0</span></div>
<div class="tp" id="tp"></div>
<div class="sw" id="sw"><svg id="svg"></svg></div>
<script>
const D=JSON.parse('{tj}');
const T=Object.keys(D);let C=T[0]||'';
const sw=document.getElementById('sw'),svg=document.getElementById('svg'),tp=document.getElementById('tp');
let sc=0.75;const NW=140,NH=34,LH=85,NS=20,DS=22;
const col={{root:'#4caf50',claim_type:'#9c27b0',decision:'#2196f3',branch_yes:'#4caf50',branch_no:'#f44336',branch_unsure:'#ff9800',action:'#607d8b',step:'#607d8b',reference:'#e91e63',linked_procedure:'#00bcd4'}};
function lay(n,l,lo){{if(!n)return{{w:0,nodes:[],edges:[],d:0}};const ch=n.children||[];let nodes=[],edges=[],md=l;
if(!ch.length){{const x=lo+NW/2,y=l*LH+50;nodes.push({{...n,x,y}});return{{w:NW+NS,nodes,edges,d:l}};}}
let co=lo,cr=[];for(const c of ch){{const r=lay(c,l+1,co);cr.push(r);nodes=nodes.concat(r.nodes);edges=edges.concat(r.edges);co+=r.w;md=Math.max(md,r.d);}}
const tw=cr.reduce((s,r)=>s+r.w,0),fx=cr[0].nodes[0]?.x||lo,lx=cr[cr.length-1].nodes[0]?.x||lo,cx=(fx+lx)/2,y=l*LH+50;
nodes.unshift({{...n,x:cx,y}});for(const c of ch){{const cn=nodes.find(nd=>nd.id===c.id);if(cn)edges.push({{fx:cx,fy:y,tx:cn.x,ty:cn.y,lb:c.edgeLabel||'',pt:n.type,dl:c.type==='linked_procedure'}});}}
return{{w:Math.max(tw,NW+NS),nodes,edges,d:md}};}}
function rnd(){{const t=D[C];if(!t){{svg.innerHTML='<text x="50" y="100" fill="#666">No data</text>';return;}}
const r=lay(t,0,80),{{nodes,edges,d}}=r;document.getElementById('nc').textContent=nodes.length;document.getElementById('dp').textContent=d;
let mx=0,my=0;nodes.forEach(n=>{{mx=Math.max(mx,n.x+NW);my=Math.max(my,n.y+NH+50);}});const w=Math.max(mx+100,800),h=Math.max(my+80,600);
svg.setAttribute('width',w*sc);svg.setAttribute('height',h*sc);svg.setAttribute('viewBox','0 0 '+w+' '+h);
let s='<defs><filter id="sh"><feDropShadow dx="2" dy="2" stdDeviation="3" flood-opacity="0.2"/></filter></defs>';
edges.forEach(e=>{{const id=e.pt==='decision',fy=e.fy+(id?DS/2+6:NH/2),ty=e.ty-NH/2-3,my=(fy+ty)/2;
const stk=e.dl?'#e91e63':'#999',sw=e.dl?3:2,da=e.dl?'5,3':'none';
s+='<path d="M '+e.fx+' '+fy+' C '+e.fx+' '+my+','+e.tx+' '+my+','+e.tx+' '+ty+'" fill="none" stroke="'+stk+'" stroke-width="'+sw+'" stroke-dasharray="'+da+'"/>';
if(e.lb){{const lx=(e.fx+e.tx)/2,ly=my-5,lc=e.lb==='YES'?'#4caf50':e.lb==='NO'?'#f44336':'#ff9800';s+='<text x="'+lx+'" y="'+ly+'" text-anchor="middle" font-size="9" font-weight="600" fill="'+lc+'">'+e.lb+'</text>';}}
}});
nodes.forEach(n=>{{const c=col[n.type]||col.step,id=n.type==='decision',il=n.type==='linked_procedure';
const ec=(n.fullContent||n.name||''),pc=n.procedureCode||'';
if(id){{s+='<g style="cursor:pointer" onmouseover="stp(event,\\''+n.type+'\\',\\''+ec+'\\',\\''+pc+'\\')" onmouseout="htp()">';
s+='<rect x="'+(n.x-DS/2)+'" y="'+(n.y-DS/2)+'" width="'+DS+'" height="'+DS+'" fill="'+c+'" stroke="#fff" stroke-width="2" rx="3" transform="rotate(45 '+n.x+' '+n.y+')" filter="url(#sh)"/>';
const lb=n.name.length>22?n.name.slice(0,22)+'...':n.name;s+='<text x="'+n.x+'" y="'+(n.y+DS/2+14)+'" text-anchor="middle" font-size="9" fill="#333" font-weight="500">'+lb+'</text></g>';
}}else if(il){{s+='<g style="cursor:pointer" onmouseover="stp(event,\\''+n.type+'\\',\\''+ec+'\\',\\''+pc+'\\')" onmouseout="htp()" onclick="jump(\\''+pc+'\\')">';
s+='<rect x="'+(n.x-NW/2)+'" y="'+(n.y-NH/2)+'" width="'+NW+'" height="'+NH+'" fill="'+c+'" stroke="#006064" stroke-width="2" stroke-dasharray="4,2" rx="8" filter="url(#sh)"/>';
s+='<text x="'+(n.x-NW/2+8)+'" y="'+n.y+'" font-size="12" fill="#fff">&#x1F517;</text>';
const lb=n.name.length>18?n.name.slice(0,18)+'...':n.name;s+='<text x="'+(n.x+8)+'" y="'+n.y+'" text-anchor="middle" dominant-baseline="middle" font-size="9" fill="#fff" font-weight="500">'+lb+'</text></g>';
}}else{{s+='<g style="cursor:pointer" onmouseover="stp(event,\\''+n.type+'\\',\\''+ec+'\\',\\''+pc+'\\')" onmouseout="htp()">';
s+='<rect x="'+(n.x-NW/2)+'" y="'+(n.y-NH/2)+'" width="'+NW+'" height="'+NH+'" fill="'+c+'" stroke="#fff" stroke-width="2" rx="8" filter="url(#sh)"/>';
const lb=n.name.length>18?n.name.slice(0,18)+'...':n.name;s+='<text x="'+n.x+'" y="'+n.y+'" text-anchor="middle" dominant-baseline="middle" font-size="9" fill="#fff" font-weight="500">'+lb+'</text></g>';}}
}});svg.innerHTML=s;}}
function stp(e,t,c,p){{let h='<b>'+t.replace(/_/g,' ')+'</b>';if(p)h+=' <span class="pc">('+p+')</span>';h+='<br>'+c;
tp.innerHTML=h;tp.style.display='block';let x=e.clientX+15,y=e.clientY+15;if(x+350>window.innerWidth)x=e.clientX-350;if(y+100>window.innerHeight)y=e.clientY-100;
tp.style.left=x+'px';tp.style.top=y+'px';}}
function htp(){{tp.style.display='none';}}
function show(ct){{C=ct;document.querySelectorAll('.cb').forEach(b=>b.classList.toggle('active',b.textContent===ct));rst();}}
function jump(pc){{for(const[n,t]of Object.entries(D))if(n.includes(pc)){{show(n);return;}}const ln='[Link] '+pc;if(D[ln])show(ln);}}
function zI(){{sc=Math.min(sc*1.2,2.5);rnd();}}
function zO(){{sc=Math.max(sc/1.2,0.25);rnd();}}
function rst(){{sc=0.75;sw.scrollTo(0,0);rnd();}}
document.addEventListener('keydown',e=>{{if(e.key==='+'||e.key==='=')zI();else if(e.key==='-')zO();else if(e.key==='0')rst();}});
sw.addEventListener('wheel',e=>{{if(e.ctrlKey){{e.preventDefault();e.deltaY<0?zI():zO();}}}});
rnd();
</script></body></html>'''

class WorldNetworkProcessor:
    def __init__(self,pdir:Optional[str]=None):
        self.parser=SOPParser();self.builder=WorldNetworkBuilder()
        self.resolver=DeepLinkResolver(pdir);self.pdir=pdir
    def process(self,pdf:str,outdir:str,max_d:int=3):
        print("="*70);print("WORLD NETWORK BUILDER");print("="*70)
        os.makedirs(outdir,exist_ok=True)
        print("\n[1] Extracting PDF...");txt=self._extract(pdf);print(f"   {len(txt):,} chars")
        print("\n[2] Parsing...");parsed=self.parser.parse(txt)
        info=parsed.get('document_info',{});doc_id=info.get('document_id','UNK');doc_name=info.get('title','Untitled')
        print(f"   {doc_name} ({doc_id})");print(f"   {len(parsed.get('sections',[]))} sections, {len(parsed.get('procedure_references',[]))} refs")
        print("\n[3] Building network...");net=self.builder.build(parsed,doc_id,doc_name);print(f"   {len(net.nodes)} nodes, {len(net.edges)} edges")
        if self.pdir:
            print(f"\n[4] Deep linking (depth={max_d})...");net=self.resolver.resolve_all(net,max_d)
            print(f"   Total: {len(net.nodes)} nodes, {len(net.linked_procedures)} linked")
        print("\n[5] Saving...")
        with open(os.path.join(outdir,'world_network.json'),'w',encoding='utf-8')as f:json.dump(net.to_dict(),f,indent=2,ensure_ascii=False)
        with open(os.path.join(outdir,'world_network_tree.html'),'w',encoding='utf-8')as f:f.write(generate_html(net))
        print("\n"+"="*70);print("COMPLETE");print(f"Output: {outdir}");return net
    def _extract(self,pdf:str)->str:
        try:import pymupdf4llm;return pymupdf4llm.to_markdown(pdf)
        except:import fitz;doc=fitz.open(pdf);t="".join(p.get_text()for p in doc);doc.close();return t

def main():
    import argparse
    p=argparse.ArgumentParser(description='World Network Builder')
    p.add_argument('pdf',help='PDF file');p.add_argument('outdir',help='Output dir')
    p.add_argument('--deep-link-dir','-d',dest='pdir',help='Procedures dir for deep linking')
    p.add_argument('--max-depth','-m',type=int,default=3,help='Max depth (default:3)')
    a=p.parse_args()
    if not os.path.exists(a.pdf):print(f"Error: {a.pdf} not found");sys.exit(1)
    WorldNetworkProcessor(pdir=a.pdir).process(a.pdf,a.outdir,a.max_depth)

if __name__=="__main__":main()
