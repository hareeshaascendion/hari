"""
Process P966 SOP - BlueCard Claims Determination
================================================
This script processes the BC - Determine If BlueCard Claim (P966) document
and generates the World Network graph for each claim type.

USAGE:
    python process_p966.py [pdf_path]
    
    If pdf_path is not provided, it will look in default locations.
"""

import os
import sys
import json

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from world_network_builder_v2 import (
    SOPToWorldNetworkProcessorV2,
    GraphVisualizerV2,
    DecisionTreeFormatter
)


def extract_pdf_content(pdf_path: str) -> str:
    """
    Extract markdown content from PDF using pymupdf4llm.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        Markdown string extracted from PDF
    """
    try:
        import pymupdf4llm
        print(f"📄 Extracting content from: {pdf_path}")
        markdown = pymupdf4llm.to_markdown(pdf_path)
        print(f"   ✓ Extracted {len(markdown):,} characters")
        return markdown
    except ImportError:
        print("❌ pymupdf4llm not installed. Installing...")
        os.system("pip install pymupdf4llm --break-system-packages -q")
        import pymupdf4llm
        return pymupdf4llm.to_markdown(pdf_path)
    except Exception as e:
        print(f"❌ Error extracting PDF: {e}")
        return ""


def save_outputs(result: dict, output_dir: str):
    """Save all outputs to files"""
    os.makedirs(output_dir, exist_ok=True)
    
    network = result['world_network']
    
    print(f"\n📁 Saving outputs to: {output_dir}")
    
    # 1. Save World Network JSON (main graph)
    wn_path = os.path.join(output_dir, 'world_network_v2.json')
    with open(wn_path, 'w', encoding='utf-8') as f:
        json.dump(network.to_dict(), f, indent=2, default=str)
    print(f"   ✓ World Network: world_network_v2.json")
    
    # 2. Save Parsed Data JSON
    parsed_path = os.path.join(output_dir, 'parsed_sop_v2.json')
    with open(parsed_path, 'w', encoding='utf-8') as f:
        json.dump(result['parsed_data'], f, indent=2, default=str)
    print(f"   ✓ Parsed SOP: parsed_sop_v2.json")
    
    # 3. Save Observation Network JSON (entities)
    on_path = os.path.join(output_dir, 'observation_network_v2.json')
    with open(on_path, 'w', encoding='utf-8') as f:
        json.dump(result['observation_network'].to_dict(), f, indent=2, default=str)
    print(f"   ✓ Observation Network: observation_network_v2.json")
    
    # 4. Save Decision Tree Summary (human-readable)
    dt_path = os.path.join(output_dir, 'decision_tree_v2.txt')
    with open(dt_path, 'w', encoding='utf-8') as f:
        f.write(result['decision_tree'])
    print(f"   ✓ Decision Tree: decision_tree_v2.txt")
    
    # 5. Save Full Mermaid Diagram
    mermaid_path = os.path.join(output_dir, 'flowchart_all.mermaid')
    with open(mermaid_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['mermaid'])
    print(f"   ✓ Mermaid (all): flowchart_all.mermaid")
    
    # 6. Save Full GraphViz DOT
    dot_path = os.path.join(output_dir, 'flowchart_all.dot')
    with open(dot_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['graphviz'])
    print(f"   ✓ GraphViz (all): flowchart_all.dot")
    
    # 7. Save Interactive HTML Visualization
    html_path = os.path.join(output_dir, 'world_network_interactive.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['html'])
    print(f"   ✓ Interactive HTML: world_network_interactive.html")
    
    # 8. Save Per-Claim-Type Graphs (SEPARATE GRAPHS FOR EACH CLAIM TYPE)
    claim_types_dir = os.path.join(output_dir, 'by_claim_type')
    os.makedirs(claim_types_dir, exist_ok=True)
    
    print(f"\n   📂 Per-Claim-Type Graphs: by_claim_type/")
    
    for claim_type in result['visualizations']['by_claim_type']['mermaid'].keys():
        # Sanitize filename
        safe_name = claim_type.replace('/', '_').replace(' ', '_').replace('(', '').replace(')', '')
        
        # Save Mermaid for this claim type
        ct_mermaid = os.path.join(claim_types_dir, f'{safe_name}.mermaid')
        with open(ct_mermaid, 'w', encoding='utf-8') as f:
            f.write(result['visualizations']['by_claim_type']['mermaid'][claim_type])
        
        # Save GraphViz DOT for this claim type
        ct_dot = os.path.join(claim_types_dir, f'{safe_name}.dot')
        with open(ct_dot, 'w', encoding='utf-8') as f:
            f.write(result['visualizations']['by_claim_type']['graphviz'][claim_type])
        
        # Save individual JSON subgraph for this claim type
        subgraph = network.get_claim_type_graph(claim_type)
        ct_json = os.path.join(claim_types_dir, f'{safe_name}.json')
        with open(ct_json, 'w', encoding='utf-8') as f:
            json.dump(subgraph, f, indent=2, default=str)
        
        print(f"      ✓ {claim_type}")
    
    # 9. Save Statistics
    stats_path = os.path.join(output_dir, 'statistics_v2.json')
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(result['statistics'], f, indent=2)
    print(f"\n   ✓ Statistics: statistics_v2.json")
    
    # 10. Save Deep Links Report (procedure references for recursive crawling)
    deep_links_path = os.path.join(output_dir, 'deep_links.json')
    deep_links = {
        'document_id': network.document_id,
        'document_name': network.document_name,
        'references': {k: v.to_dict() for k, v in network.procedure_refs.items()},
        'total_references': len(network.procedure_refs),
        'pending_resolution': [
            {
                'code': ref.procedure_code,
                'name': ref.procedure_name,
                'context': ref.source_context,
                'url': ref.url
            }
            for ref in network.procedure_refs.values()
            if not ref.resolved
        ]
    }
    with open(deep_links_path, 'w', encoding='utf-8') as f:
        json.dump(deep_links, f, indent=2)
    print(f"   ✓ Deep Links: deep_links.json")
    
    # 11. Save the raw extracted markdown for reference
    if 'raw_markdown' in result:
        md_path = os.path.join(output_dir, 'extracted_content.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(result['raw_markdown'])
        print(f"   ✓ Raw Markdown: extracted_content.md")
    
    return output_dir


def print_summary(result: dict):
    """Print summary of processing results"""
    stats = result['statistics']
    network = result['world_network']
    
    print("\n" + "=" * 80)
    print("✅ PROCESSING COMPLETE")
    print("=" * 80)
    
    print(f"\n📋 DOCUMENT INFO:")
    print(f"   Name: {network.document_name}")
    print(f"   ID: {network.document_id}")
    print(f"   Version: {network.current_version}")
    
    print(f"\n📊 WORLD NETWORK STATISTICS:")
    print(f"   Total Nodes: {stats['total_nodes']}")
    print(f"   Total Edges: {stats['total_edges']}")
    print(f"   Decision Points: {stats['decision_points']}")
    
    print(f"\n📋 CLAIM TYPES ({stats['num_claim_types']}):")
    for ct in stats['claim_types']:
        depth = stats['max_depths'].get(ct, 0)
        print(f"   • {ct} (max depth: {depth})")
    
    print(f"\n🔗 DEEP LINK REFERENCES ({stats['procedure_references']}):")
    for ref_id, ref in network.procedure_refs.items():
        status = "✓ Resolved" if ref.resolved else "⏳ Pending"
        print(f"   {status}: {ref.procedure_code}")
        if ref.procedure_name:
            print(f"            └─ {ref.procedure_name}")
    
    print(f"\n🏷️ ENTITIES ({stats['unique_entities']}):")
    entity_counts = {}
    for entity in network.entities.values():
        etype = entity.entity_type
        entity_counts[etype] = entity_counts.get(etype, 0) + 1
    for etype, count in sorted(entity_counts.items()):
        print(f"   {etype}: {count}")
    
    if stats.get('lookup_tables'):
        print(f"\n📑 LOOKUP TABLES:")
        for table_name, count in stats['lookup_tables'].items():
            print(f"   • {table_name}: {count} entries")
    
    print("\n" + "=" * 80)


def main(pdf_path: str = None):
    """
    Main entry point for processing P966 SOP.
    
    Args:
        pdf_path: Path to the PDF file. If None, searches default locations.
    """
    print("=" * 80)
    print("PHASE 1: WORLD NETWORK BUILDER v2.0")
    print("Processing BC - Determine If BlueCard Claim (P966)")
    print("=" * 80)
    
    # Find PDF file if not provided
    if pdf_path is None:
        pdf_paths = [
            '/mnt/user-data/uploads/BC_Determine_If_BlueCard_Claim_P966.pdf',
            '/home/claude/BC - Determine If BlueCard Claim - P966_v4.pdf',
            'BC - Determine If BlueCard Claim - P966_v4.pdf',
            'BC_Determine_If_BlueCard_Claim_P966.pdf'
        ]
        
        for path in pdf_paths:
            if os.path.exists(path):
                pdf_path = path
                break
    
    if not pdf_path or not os.path.exists(pdf_path):
        print("❌ PDF file not found!")
        print("   Please provide the path to the PDF file as an argument:")
        print("   python process_p966.py /path/to/your/file.pdf")
        sys.exit(1)
    
    print(f"\n📄 PDF Source: {pdf_path}")
    
    # Step 1: Extract content from PDF
    print("\n" + "-" * 40)
    print("STEP 1: Extracting PDF Content")
    print("-" * 40)
    
    markdown_content = extract_pdf_content(pdf_path)
    
    if not markdown_content:
        print("❌ Failed to extract content from PDF")
        sys.exit(1)
    
    # Step 2: Process through World Network Builder
    print("\n" + "-" * 40)
    print("STEP 2: Building World Network")
    print("-" * 40)
    
    processor = SOPToWorldNetworkProcessorV2()
    result = processor.process(markdown_content, document_id="P966")
    
    # Store raw markdown in result for saving
    result['raw_markdown'] = markdown_content
    
    # Step 3: Save all outputs
    print("\n" + "-" * 40)
    print("STEP 3: Saving Outputs")
    print("-" * 40)
    
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
    save_outputs(result, output_dir)
    
    # Step 4: Print summary
    print_summary(result)
    
    print(f"\n📁 All outputs saved to: {output_dir}")
    print("\nKey files:")
    print("  • world_network_v2.json     - Complete graph structure")
    print("  • decision_tree_v2.txt      - Human-readable decision tree")
    print("  • observation_network_v2.json - Extracted entities (Premera-wide)")
    print("  • deep_links.json           - Procedure references for crawling")
    print("  • by_claim_type/            - Separate graphs for each claim type")
    
    return result


if __name__ == "__main__":
    # Get PDF path from command line if provided
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else None
    main(pdf_path)
