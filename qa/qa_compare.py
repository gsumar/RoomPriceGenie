"""
QA Comparison Script
Compares medallion pipeline output with pure Python QA output.
"""

import csv
from pathlib import Path


def load_csv(path):
    """Load CSV and return rows as dictionaries."""
    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def compare_reports(medallion_path, qa_path, tolerance=0.01):
    """Compare medallion report with QA report."""
    medallion = load_csv(medallion_path)
    qa = load_csv(qa_path)
    
    print("=" * 80)
    print("QA COMPARISON REPORT")
    print("=" * 80)
    print()
    print(f"Medallion report:  {medallion_path}")
    print(f"QA report:         {qa_path}")
    print()
    
    # Check row count
    print(f"Row count:")
    print(f"  Medallion: {len(medallion)} rows")
    print(f"  QA:        {len(qa)} rows")
    
    if len(medallion) != len(qa):
        print(f"  ❌ MISMATCH: Different number of rows!")
        return False
    else:
        print(f"  ✅ MATCH")
    print()
    
    # Compare each row with detailed output
    mismatches = []
    for i, (m_row, q_row) in enumerate(zip(medallion, qa)):
        m_night = m_row['NIGHT_OF_STAY']
        q_night = q_row['NIGHT_OF_STAY']
        
        if m_night != q_night:
            mismatches.append({
                'type': 'date',
                'row': i,
                'medallion': m_night,
                'qa': q_night
            })
            continue
        
        try:
            m_occ = float(m_row['OCCUPANCY_PERCENTAGE'])
            q_occ = float(q_row['OCCUPANCY_PERCENTAGE'])
            m_rev = float(m_row['TOTAL_NET_REVENUE'])
            q_rev = float(q_row['TOTAL_NET_REVENUE'])
            m_adr = float(m_row['ADR'])
            q_adr = float(q_row['ADR'])
            
            occ_diff = abs(m_occ - q_occ)
            rev_diff = abs(m_rev - q_rev)
            adr_diff = abs(m_adr - q_adr)
            
            if occ_diff > tolerance:
                mismatches.append({
                    'type': 'occupancy',
                    'row': i,
                    'date': m_night,
                    'medallion': m_occ,
                    'qa': q_occ,
                    'diff': occ_diff
                })
            if rev_diff > tolerance:
                mismatches.append({
                    'type': 'revenue',
                    'row': i,
                    'date': m_night,
                    'medallion': m_rev,
                    'qa': q_rev,
                    'diff': rev_diff
                })
            if adr_diff > tolerance:
                mismatches.append({
                    'type': 'adr',
                    'row': i,
                    'date': m_night,
                    'medallion': m_adr,
                    'qa': q_adr,
                    'diff': adr_diff
                })
        except ValueError:
            mismatches.append({'type': 'parse', 'row': i})
    
    print(f"Row-by-row comparison (tolerance={tolerance}):")
    if mismatches:
        print(f"  ❌ FOUND {len(mismatches)} MISMATCHES:")
        for mismatch in mismatches[:15]:  # Show first 15
            if mismatch['type'] == 'date':
                print(f"    - Row {mismatch['row']}: Date {mismatch['medallion']} vs {mismatch['qa']}")
            else:
                print(f"    - Row {mismatch['row']} ({mismatch.get('date', '?')}): {mismatch['type'].upper()} medallion={mismatch.get('medallion', '?')} qa={mismatch.get('qa', '?')}")
        if len(mismatches) > 15:
            print(f"    ... and {len(mismatches) - 15} more")
        return False
    else:
        print(f"  ✅ ALL ROWS MATCH")
    
    print()
    return True


def main():
    root = Path(__file__).parents[1]
    medallion_path = root / 'reports' / 'kpi_1035_2026_05_01to_2026_05_31_.csv'
    qa_path = root / 'qa' / 'kpi_1035_qa_pure_python.csv'
    
    if not medallion_path.exists():
        print(f"❌ Medallion report not found: {medallion_path}")
        return False
    
    if not qa_path.exists():
        print(f"❌ QA report not found: {qa_path}")
        print(f"   Run: python qa/qa_pure_python.py")
        return False
    
    result = compare_reports(medallion_path, qa_path)
    
    if result:
        print("=" * 80)
        print("✅ VALIDATION PASSED - Both outputs match!")
        print("=" * 80)
    else:
        print("=" * 80)
        print("❌ VALIDATION FAILED - Outputs differ!")
        print("=" * 80)
    
    return result


if __name__ == '__main__':
    main()

