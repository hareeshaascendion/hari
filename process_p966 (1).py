"""
Process P966 SOP - BlueCard Claims Determination
================================================
This script processes the BC - Determine If BlueCard Claim (P966) document
and generates the World Network graph for each claim type.

Usage:
    python process_p966.py [pdf_path] [output_dir]
    
    If no arguments provided, uses defaults.
"""

import os
import sys
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from world_network_builder_v2 import (
    SOPToWorldNetworkProcessorV2,
    GraphVisualizerV2,
    DecisionTreeFormatter
)


def extract_pdf_content(pdf_path: str) -> str:
    """Extract markdown content from PDF using pymupdf4llm"""
    try:
        import pymupdf4llm
        print(f"Extracting content from: {pdf_path}")
        markdown = pymupdf4llm.to_markdown(pdf_path)
        print(f"✓ Extracted {len(markdown):,} characters")
        return markdown
    except ImportError:
        print("❌ pymupdf4llm not installed. Install with: pip install pymupdf4llm")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error extracting PDF: {e}")
        sys.exit(1)


def save_outputs(result: dict, output_dir: str):
    """Save all outputs to files"""
    os.makedirs(output_dir, exist_ok=True)
    
    network = result['world_network']
    
    print(f"\n📁 Saving outputs to: {output_dir}")
    print("-" * 50)
    
    # 1. Save World Network JSON
    wn_path = os.path.join(output_dir, 'world_network_v2.json')
    with open(wn_path, 'w', encoding='utf-8') as f:
        json.dump(network.to_dict(), f, indent=2, default=str)
    print(f"✓ World Network JSON: {wn_path}")
    
    # 2. Save Parsed Data JSON
    parsed_path = os.path.join(output_dir, 'parsed_sop_v2.json')
    with open(parsed_path, 'w', encoding='utf-8') as f:
        json.dump(result['parsed_data'], f, indent=2, default=str)
    print(f"✓ Parsed SOP JSON: {parsed_path}")
    
    # 3. Save Observation Network JSON
    on_path = os.path.join(output_dir, 'observation_network_v2.json')
    with open(on_path, 'w', encoding='utf-8') as f:
        json.dump(result['observation_network'].to_dict(), f, indent=2, default=str)
    print(f"✓ Observation Network JSON: {on_path}")
    
    # 4. Save Decision Tree Summary (Human Readable)
    dt_path = os.path.join(output_dir, 'decision_tree_v2.txt')
    with open(dt_path, 'w', encoding='utf-8') as f:
        f.write(result['decision_tree'])
    print(f"✓ Decision Tree TXT: {dt_path}")
    
    # 5. Save Full Mermaid Diagram
    mermaid_path = os.path.join(output_dir, 'flowchart_all.mermaid')
    with open(mermaid_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['mermaid'])
    print(f"✓ Mermaid Diagram: {mermaid_path}")
    
    # 6. Save Full GraphViz DOT
    dot_path = os.path.join(output_dir, 'flowchart_all.dot')
    with open(dot_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['graphviz'])
    print(f"✓ GraphViz DOT: {dot_path}")
    
    # 7. Save Interactive HTML
    html_path = os.path.join(output_dir, 'world_network_interactive.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(result['visualizations']['html'])
    print(f"✓ Interactive HTML: {html_path}")
    
    # 8. Save Per-Claim-Type Graphs
    claim_types_dir = os.path.join(output_dir, 'by_claim_type')
    os.makedirs(claim_types_dir, exist_ok=True)
    
    print(f"\n📊 Saving per-claim-type graphs to: {claim_types_dir}")
    
    for claim_type, mermaid_content in result['visualizations']['by_claim_type']['mermaid'].items():
        # Sanitize filename
        safe_name = claim_type.replace('/', '_').replace(' ', '_').replace('(', '').replace(')', '')
        safe_name = safe_name.replace(',', '').replace("'", '')
        
        # Save Mermaid
        ct_mermaid = os.path.join(claim_types_dir, f'{safe_name}.mermaid')
        with open(ct_mermaid, 'w', encoding='utf-8') as f:
            f.write(mermaid_content)
        
        # Save GraphViz
        ct_dot = os.path.join(claim_types_dir, f'{safe_name}.dot')
        with open(ct_dot, 'w', encoding='utf-8') as f:
            f.write(result['visualizations']['by_claim_type']['graphviz'][claim_type])
        
        # Save individual JSON subgraph
        subgraph = network.get_claim_type_graph(claim_type)
        ct_json = os.path.join(claim_types_dir, f'{safe_name}.json')
        with open(ct_json, 'w', encoding='utf-8') as f:
            json.dump(subgraph, f, indent=2, default=str)
        
        print(f"   ✓ {claim_type}")
    
    # 9. Save Statistics
    stats_path = os.path.join(output_dir, 'statistics_v2.json')
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(result['statistics'], f, indent=2)
    print(f"\n✓ Statistics JSON: {stats_path}")
    
    # 10. Save Deep Links Report
    deep_links_path = os.path.join(output_dir, 'deep_links.json')
    deep_links = {
        'document_id': network.document_id,
        'document_name': network.document_name,
        'total_references': len(network.procedure_refs),
        'references': {k: v.to_dict() for k, v in network.procedure_refs.items()},
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
    print(f"✓ Deep Links JSON: {deep_links_path}")
    
    # 11. Save raw markdown content
    raw_md_path = os.path.join(output_dir, 'source_markdown.md')
    if 'raw_markdown' in result:
        with open(raw_md_path, 'w', encoding='utf-8') as f:
            f.write(result['raw_markdown'])
        print(f"✓ Source Markdown: {raw_md_path}")
    
    return output_dir


def print_summary(result: dict):
    """Print summary of processing results"""
    stats = result['statistics']
    network = result['world_network']
    
    print("\n" + "=" * 80)
    print("✅ PROCESSING COMPLETE - SUMMARY")
    print("=" * 80)
    
    print(f"\n📄 DOCUMENT INFO:")
    print(f"   Name: {network.document_name}")
    print(f"   Document ID: {network.document_id}")
    print(f"   Current Version: {network.current_version}")
    
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
        print(f"   • {ref.procedure_code} [{status}]")
        if ref.procedure_name:
            print(f"     └─ {ref.procedure_name[:60]}")
    
    print(f"\n🏷️ ENTITIES ({stats['unique_entities']}):")
    entity_counts = {}
    for entity in network.entities.values():
        etype = entity.entity_type
        entity_counts[etype] = entity_counts.get(etype, 0) + 1
    for etype, count in sorted(entity_counts.items()):
        print(f"   • {etype}: {count}")
    
    if stats.get('lookup_tables'):
        print(f"\n📑 LOOKUP TABLES:")
        for table_name, count in stats['lookup_tables'].items():
            print(f"   • {table_name}: {count} entries")
    
    # Version history
    if network.versions:
        print(f"\n📜 VERSION HISTORY:")
        for v in network.versions[:5]:  # Show last 5
            print(f"   • v{v.revision} ({v.date})")
            print(f"     {v.description[:70]}...")
    
    print("\n" + "=" * 80)


def main(pdf_path: str = None, output_dir: str = None):
    """
    Main entry point for processing P966 SOP.
    
    Args:
        pdf_path: Path to the PDF file (optional, will use default)
        output_dir: Output directory (optional, will use default)
    """
    print("=" * 80)
    print("PHASE 1: WORLD NETWORK BUILDER v2.0")
    print("Processing BC - Determine If BlueCard Claim (P966)")
    print("=" * 80)
    
    # Determine PDF path
    if pdf_path is None:
        # Check common locations
        pdf_paths = [
            '/mnt/user-data/uploads/BC_Determine_If_BlueCard_Claim_P966.pdf',
            '/home/claude/BC - Determine If BlueCard Claim - P966_v4.pdf',
            './BC_Determine_If_BlueCard_Claim_P966.pdf',
            './BC - Determine If BlueCard Claim - P966_v4.pdf'
        ]
        
        for path in pdf_paths:
            if os.path.exists(path):
                pdf_path = path
                break
        
        if pdf_path is None:
            print("❌ PDF file not found. Please provide the path as argument.")
            print("   Usage: python process_p966.py <pdf_path> [output_dir]")
            sys.exit(1)
    
    # Verify PDF exists
    if not os.path.exists(pdf_path):
        print(f"❌ PDF file not found: {pdf_path}")
        sys.exit(1)
    
    print(f"\n📄 PDF File: {pdf_path}")
    
    # Determine output directory
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output_v2')
    
    print(f"📁 Output Directory: {output_dir}")
    
    # Step 1: Extract PDF content
    print("\n" + "-" * 50)
    print("Step 1: Extracting PDF content...")
    print("-" * 50)
    markdown_content = extract_pdf_content(pdf_path)
    
    # Step 2: Process with World Network Builder
    print("\n" + "-" * 50)
    print("Step 2: Building World Network...")
    print("-" * 50)
    
    processor = SOPToWorldNetworkProcessorV2()
    result = processor.process(markdown_content, document_id="P966")
    
    # Add raw markdown to result for saving
    result['raw_markdown'] = markdown_content
    
    # Step 3: Save outputs
    print("\n" + "-" * 50)
    print("Step 3: Saving outputs...")
    print("-" * 50)
    save_outputs(result, output_dir)
    
    # Step 4: Print summary
    print_summary(result)
    
    print(f"\n🎉 All outputs saved to: {output_dir}")
    
    return result


if __name__ == "__main__":
    # Parse command line arguments
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else None
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    main(pdf_path, output_dir)
    
